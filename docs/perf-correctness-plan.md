# Plan Perf & Correctness — ArcRun

Issu de l'audit de performance du 2026-06-11. Ce document est la référence de cohérence
entre les lots : il consigne les designs **et les raisons** des choix, pour ne pas
réintroduire un problème déjà tranché.

## Contrat API cible (décisions actées)

1. **Réponse API = état durable.** Quand `PATCH /task/{id}` répond 200, la transition de
   statut et toute la propagation aux enfants sont committées.
2. **Webhooks = notifications at-least-once.** Tout événement de cycle de vie (end, cancel)
   sera livré au moins une fois, y compris après crash/redéploiement. Dédoublonnage côté
   consommateur via le header `Idempotency-Key`. Pas d'ordre garanti **entre tâches**
   (le `on_success` du parent peut arriver après le `on_start` de l'enfant — l'ordre causal
   vit dans l'état, pas dans les notifications). Ordre garanti **par tâche** : `start` livré
   avant `end`.
3. **L'état est la vérité.** Un consommateur qui rate un webhook réconcilie via le
   `?handle=` (GET /task/{id}). Les états terminaux sont immuables.
4. **`on_start` est du control-flow, pas une notification** : sa réponse pilote
   l'orchestrateur (action cancel, échec ⇒ tâche Failed). Il reste synchrone dans la
   `start_loop` et ne passera PAS par l'outbox.

---

## Lot 0 — Quick wins — ✅ FAIT (2026-06-11, dans le working tree)

1. `src/main.rs` : un seul `ActionExecutor` partagé (workers + AppState) — un seul pool reqwest.
2. `src/workers/start_loop.rs` : logs « blocked by rule » passés de `warn` à `debug`
   (les métriques Prometheus `record_task_blocked_by_concurrency` restent).
3. `src/db/task_query.rs` : filtres `LIKE` de `list_task_filtered_paged` appliqués
   seulement si non vides (colonnes NOT NULL ⇒ équivalent).
4. Migration `2026-06-11-000001_drop_redundant_status_index` : drop de `idx_task_status`
   (couvert par `idx_task_priority(status,…)` et `idx_task_status_kind(status,kind)`).
   ⚠️ Avant déploiement prod : valider par `EXPLAIN` que les requêtes de la `timeout_loop`
   basculent sur `idx_task_priority`. Rollback : `down.sql`.

---

## Lot 1 — Boucle de claim paginée — ✅ FAIT (2026-06-11)

### Problème
`list_all_pending` (`src/db/task_query.rs`) charge **toutes** les tâches Pending (lignes
complètes avec JSONB) à chaque itération (1 s) : le `.limit()` a été retiré volontairement,
et `_batch_size` dans `claim_phase` (`src/workers/start_loop.rs`) est ignoré.

### Pourquoi pas un simple LIMIT (décision actée — NE PAS régresser)
Head-of-line blocking : avec `LIMIT 50`, si les 50 premières tâches (par priorité/ancienneté)
sont bloquées par une règle de concurrence, une 51e tâche éligible ne serait **jamais** vue.
C'est la raison pour laquelle le LIMIT avait été retiré.

### Design retenu : ne pas limiter la visibilité, limiter la mémoire
```
claim_cap  = WORKER_START_BATCH_SIZE  (config existante, redevient utile : cap de claims/itération)
page_size  = constante interne (~500)  (borne la mémoire)

cursor = début
loop:
    page = Pending WHERE keyset > cursor ORDER BY priority DESC, created_at ASC, id ASC LIMIT page_size
    pour chaque tâche : prefilter ko-cache → claim_task_with_rules → claimed++
    si claimed >= claim_cap : break    # on s'arrête parce qu'on a du travail, pas par aveuglement
    si page incomplète : break          # backlog entièrement parcouru
    cursor = dernière ligne de la page
```
- L'arrêt anticipé ne se déclenche **que** si on a réellement claim `claim_cap` tâches.
  Sinon on parcourt tout le backlog ⇒ aucune famine possible.
- Le cache `ko` (lock keys bloquées) est conservé **entre les pages** d'une même itération :
  scanner 50k tâches bloquées ≈ ~100 requêtes de pagination indexées + 1 check DB par classe
  de règle.
- **Keyset, pas OFFSET.** Attention à l'ordre mixte (`priority DESC, created_at ASC, id ASC`) :
  pas de comparaison de tuple directe en SQL, il faut le prédicat développé :
  `(priority < p) OR (priority = p AND created_at > c) OR (priority = p AND created_at = c AND id > i)`.
  Sert l'index `idx_task_priority(status, priority DESC, created_at ASC)`.
  Le tiebreaker `id` rend le curseur stable si des tâches sont claim entre deux pages.

### Batch-claim des tâches sans règles (dans une page)
Les runs **contigus dans l'ordre de priorité** de tâches avec `start_condition` vide sont
claim en un seul `UPDATE task SET status='claimed', last_updated=now()
WHERE id = ANY($ids) AND status='pending' RETURNING id`.

⚠️ Garde-fou (décision actée) : ne PAS batcher au-delà d'une tâche porteuse de règles.
Une tâche sans règle peut être **comptée** par la règle d'une autre (match kind/metadata) ;
batcher hors-ordre créerait une inversion de priorité (les sans-règles basse priorité
gonfleraient le compteur et bloqueraient une tâche haute priorité avec règle).

### Hors scope de ce lot
Cache conservateur des Capacity keys (un échec de capacité ⇒ skip de la classe pour le
reste de l'itération). À considérer seulement si un backlog massif bloqué par Capacity
rules apparaît en pratique (elles ne sont pas cachables dans `ko`, cf. commentaire dans
`claim_phase`).

### Tests exigés
- **Régression head-of-line** : N tâches bloquées par une règle de concurrence en tête de
  file (priorité haute), 1 tâche éligible derrière (priorité basse) ⇒ elle est claim dans
  la même itération.
- Cap respecté : pas plus de `claim_cap` claims par itération, et l'itération suivante
  reprend le reste.
- Batch-claim : ordre de priorité respecté, pas de batch au travers d'une tâche à règles.
- Les suites existantes `test_priority` et `test_concurrency` restent vertes.
- Mettre à jour la description de `WORKER_START_BATCH_SIZE` dans CLAUDE.md
  (« max claims par itération », plus « max fetched »).

---

## Lot 2 — Outbox transactionnel — ✅ FAIT (2026-06-11)

Validation : 162 tests d'intégration (156 + 6 nouveaux dans `test_outbox.rs`) + 30
unitaires verts, vérifiés indépendamment de l'agent d'implémentation. Décisions prises
à l'implémentation : enqueue inconditionnel (la boucle marque `success` les lignes sans
action) ; `next_attempt_at NOT NULL DEFAULT now()` ; payload enrichi sous une clé
réservée `arcrun` (merge non destructif dans un body custom) ; la boucle de livraison
ne traite que les triggers `end`/`cancel` (les `start` restent synchrones).

Suivi noté en relecture (non bloquant) : `run_delivery_once` exécute tout le batch
(appels HTTP compris) dans UNE transaction qui tient les locks `FOR UPDATE` — livraison
séquentielle, 1 connexion tenue pendant tout le batch, et une erreur DB en cours de
batch rollback les marks `success` déjà posés (⇒ relivraison, acceptable en
at-least-once). Si le débit de livraison devient un goulot : claim court (marquage
in-flight) puis livraison hors-tx en parallèle.

Transforme `webhook_execution` (déjà : `idempotency_key UNIQUE`, `status`, `attempts`)
en transactional outbox. Résout : pool starvation par webhooks lents dans le chemin HTTP
(`update_running_task` fire les webhooks avant de répondre, connexion tenue jusqu'à 10 s),
webhooks perdus en cas de crash post-commit, absence de retry.

1. **Migration** : valeur `exhausted` dans `webhook_execution_status` ; colonnes
   `next_attempt_at TIMESTAMPTZ`, `last_error TEXT` ; index partiel
   `(next_attempt_at) WHERE status = 'pending'`.
2. **Insertion in-tx** : `update_running_task`, `cancel_task`, `timeout_task_and_propagate`,
   `fail_task_and_propagate`, `stop_batch` + ancêtres dead-end insèrent leurs lignes
   `pending` (tâche + cascade + ancêtres) **dans la transaction** du changement de statut,
   et n'appellent plus reqwest. Exception : `on_start` reste synchrone dans la `start_loop`
   (control-flow, cf. contrat).
3. **Boucle de livraison** (5e worker, à côté de start/timeout/batch/retention) :
   `FOR UPDATE SKIP LOCKED` sur les `pending` mûrs, backoff exponentiel, max N tentatives
   puis `exhausted` + métrique. Ordre par tâche : un `end` attend que le `start` de la même
   tâche ne soit plus `pending`.
4. **Payload enrichi** : statut final + timestamp de transition dans le corps du webhook
   (en plus de `X-Task-Id`/`X-Task-Trigger`/`Idempotency-Key`).
5. **Observabilité** : `GET /webhook-deliveries?status=exhausted`, métriques
   retry/exhausted/lag de livraison.
6. **Tests** : fenêtre de crash (ligne `pending` survit et est livrée au redémarrage),
   retry après échec aval, pas de double livraison, ordre start-avant-end par tâche,
   « PATCH répond vite même si l'aval est lent ».
7. **Documentation du contrat** (section en tête de ce fichier) dans CLAUDE.md/README.

Coût accepté : une écriture par transition (dans une tx déjà ouverte) + latence de
livraison = intervalle de la boucle (configurable ; option : wake-up immédiat via canal).

---

## Lot 3 — Sur preuve de besoin uniquement

- **Insertion de batch groupée** dans `add_task` (actuellement N+1 : ~4 round-trips par
  tâche). Contraintes actées : UUID générés côté application ; batching limité aux tâches
  **sans `dedupe_strategy`** — la dédup est évaluée séquentiellement dans la transaction et
  peut matcher une tâche insérée plus tôt **dans le même batch** ; un INSERT multi-valeurs
  casserait cette sémantique.
- **Webhook `on_batch_complete`** : déclenché dans la transaction de propagation quand la
  dernière tâche du batch devient terminale (même mécanique que la détection dead-end).
  C'est la bonne réponse au besoin « signal de fin ordonné » — pas l'ordonnancement des
  livraisons.
- ~~CTE récursive pour `propagate_to_children`~~ — écarté : la logique par niveau
  (échecs `requires_success` + décréments + transition Pending) est délicate en SQL pur et
  la version actuelle est déjà batchée par niveau. Rapport risque/gain défavorable.

## Améliorations notées, non planifiées

- `BATCH_CHANNEL_CAPACITY` (défaut 100) petit pour un endpoint « high-throughput » :
  à augmenter si le PUT devient un goulot.
- Pagination OFFSET des listings : keyset si la pagination profonde devient un usage réel.
- Découplage webhook_phase / cadence de claim dans `start_loop` : devient sans objet
  une fois le Lot 2 livré (seuls les `on_start` restent dans la boucle, et l'attente de
  fin de phase est une backpressure voulue vis-à-vis de `claim_timeout` — un backlog de
  webhooks > `claim_timeout` ferait requeue des Claimed dont le webhook est encore en file).
