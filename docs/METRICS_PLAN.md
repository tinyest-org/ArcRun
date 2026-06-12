# Plan Métriques — ArcRun

> **État (2026-06-12) : TERMINÉ.** Lots M0→M4 implémentés en direct (session
> principale, sans agent). `src/metrics.rs` câblé, sampler périodique
> (`src/workers/metrics_sampler.rs`, 6ᵉ worker), doc `docs/metrics.md` à jour.
> Build + `cargo test --lib` + sous-suites d'intégration (outbox, batch_update,
> claim_loop, batch_complete) au vert. Breaking dashboards : `tasks_by_status`
> n'a plus de label `pool_exhausted` (→ `db_pool_acquire_failures_total`) et les
> métriques `worker_loop_*` portent désormais un label `loop`.

Roadmap pour combler les trous d'observabilité identifiés après le plan perf &
correctness (2026-06-12). Principe directeur : le backend n'a plus de goulot *connu* —
le prochain goulot, seules les métriques le révéleront. Chaque lot est petit,
indépendant, et priorisé par « qu'est-ce qui casserait en premier sans qu'on le voie ».

Référence de l'existant : `src/metrics.rs` (~35 métriques enregistrées). L'export HTTP
(latence/statut par endpoint) est déjà couvert par `actix-web-prom` — rien à faire là.

## État des lieux — dettes de l'existant

Découvertes en inventaire (2026-06-12) :

| Problème | Détail |
|---|---|
| Jauges mortes | `tasks_by_status` et `running_tasks_by_kind` : enregistrées, helpers `set_*` présents, **jamais appelés**. Un dashboard qui les utilise affiche du vide. |
| Histogramme mort | `task_wait_seconds` (attente Pending→Running) : enregistré, jamais observé. |
| Détournement | `handlers/mod.rs:122` incrémente `tasks_by_status{status="pool_exhausted"}` — un compteur d'épuisement de pool déguisé en label de *jauge* de statut de tâches. |
| Compteurs sans label de boucle | `worker_loop_iterations_total` / `worker_loop_duration_seconds` ne sont alimentés que par la `start_loop` ; les 4 autres boucles (timeout, delivery, batch_updater, retention) sont invisibles (la retention a ses propres métriques). |
| Erreur avalée sans métrique | `delivery_loop.rs::apply_mark` : un mark qui échoue est loggé et ignoré (design voulu, le lease relivre) — mais **aucun compteur**. Si les marks échouent en boucle, seul un grep des logs le montre. |

---

## Lot M0 — Réparer l'existant (préalable, ~petit)

> On ne construit pas de nouvelles métriques sur des fondations qui mentent.

1. **`db_pool_acquire_failures_total`** (counter) — remplace le détournement
   `tasks_by_status{status="pool_exhausted"}` dans `handlers/mod.rs::conn()`.
   ⚠️ Breaking pour tout dashboard qui lisait ce label — à mentionner dans le commit.
2. **Alimenter ou supprimer les jauges mortes.** Décision proposée : les alimenter via
   un échantillonneur périodique (voir M3.3) plutôt que les supprimer — `tasks_by_status`
   est LA jauge de santé macro (backlog Pending qui monte = problème de claim ;
   Waiting qui stagne = DAG bloqué).
3. **`task_wait_seconds`** : observer au passage Pending→Running (dans
   `set_started_task` / le claim de la `start_loop`, où `created_at` et `started_at`
   sont disponibles). C'est la métrique d'expérience utilisateur du scheduler.
4. **Label `loop` sur les métriques de boucle** : `worker_loop_iterations_total{loop=…}`,
   `worker_loop_duration_seconds{loop=…}`, alimentées par les 5 boucles.
   ⚠️ Breaking (ajout de label) — faire en même temps que le point 1 pour grouper la
   casse de dashboards en un seul déploiement.

## Lot M1 — Batch updater / `PUT /task/{id}` (priorité 1)

> Le plafond identifié : canal mpsc borné à `BATCH_CHANNEL_CAPACITY`=100 ; quand il est
> plein, `send().await` bloque le handler et le « 202 immédiat » devient lent **sans
> aucun signal**. C'est le trou le plus probable sous montée de trafic.

1. **`batch_channel_send_wait_seconds`** (histogram, buckets fins : 1ms→1s) — durée du
   `send().await` dans `batch_task_updater` (`handlers/task.rs:157`). LE signal de
   saturation : p99 > quelques ms = le canal refoule. Alerte associée : p99 > 50 ms.
2. **`batch_channel_capacity_available`** (gauge) — `sender.capacity()` échantillonné au
   moment du send (gratuit, pas de boucle dédiée). Complément visuel du précédent.
3. **`batch_update_events_total`** (counter) — débit d'entrée brut du PUT. Sans lui, on
   ne peut pas dire « le canal sature à X événements/s ».
4. **`batch_updater_flush_rows`** (histogram) + **`batch_updater_flush_duration_seconds`**
   (histogram) — taille et durée du flush DB périodique (`batch_updater.rs:61`).
   Donne le facteur d'agrégation réel (événements reçus / lignes flushées) et anticipe
   le moment où le flush de 100 ms ne tient plus dans son intervalle.
5. **`batch_updater_pending_tasks`** (gauge) — taille de la DashMap après flush.
   Mesure le « stock » de compteurs non persistés (= fenêtre de perte en cas de crash).

## Lot M2 — Outbox / delivery loop (priorité 2)

> Le contrat est at-least-once via la table `webhook_execution` ; les métriques
> actuelles (retries, exhausted, lag) ne voient que ce qui *passe* dans la boucle.
> Une ligne coincée ou un backlog qui enfle sont invisibles : le lag n'est observé
> **qu'à la livraison réussie**.

1. **`webhook_outbox_pending`** (gauge, label `state="ready"|"leased"`) — profondeur du
   backlog, un `COUNT … FILTER` par itération de la delivery loop (la requête est
   triviale, l'index partiel `idx_webhook_execution_pending_due` la sert).
   Alerte : `ready` qui croît sur 10 min = la boucle ne suit plus.
2. **`webhook_outbox_oldest_pending_age_seconds`** (gauge) — âge de la plus vieille
   ligne `pending` mûre. LE signal worst-case : une ligne bloquée (gate start-before-end
   jamais levé, endpoint en panne longue) se voit ici et nulle part ailleurs.
3. **`webhook_mark_failures_total`** (counter, label `mark="success"|"retry"|"exhausted"`)
   — dans `apply_mark` (`delivery_loop.rs`), là où l'erreur est aujourd'hui avalée.
4. **`webhook_delivery_success_total`** (counter, label `trigger`) — pendant des
   compteurs retries/exhausted existants ; permet le ratio d'échec sans passer par
   l'histogramme de lag.
5. **`webhooks_in_flight{phase="delivery"}`** — réutiliser le `WebhooksInFlightGuard`
   existant (aujourd'hui seule la phase `start` est trackée) autour de `deliver_plan`.
   Montre si on plafonne à `WEBHOOK_DELIVERY_CONCURRENCY`.

## Lot M3 — Pool DB & liveness des boucles (priorité 3)

> Les pathologies historiques du service étaient toutes des starvations de pool ; on a
> corrigé les causes connues mais on ne *mesure* toujours pas le pool. Et une boucle de
> fond morte (panic avalé, deadlock) est invisible tant que ses effets ne remontent pas.

1. **`db_pool_connections{state="in_use"|"idle"}`** (gauges) — `bb8` expose
   `Pool::state()` ; échantillonner dans le sampler du point 3.
2. **`db_pool_acquire_wait_seconds`** (histogram) — durée du `pool.get()` dans
   `handlers/mod.rs::conn()` (le point de passage unique côté HTTP) et dans les boucles.
   Complète `db_pool_acquire_failures_total` (M0.1) : on voit la dégradation *avant*
   l'échec.
3. **Échantillonneur périodique** (toutes les 15 s, tâche tokio dédiée ou greffée sur la
   `timeout_loop`) qui alimente : `tasks_by_status` (un `GROUP BY status` indexé),
   `running_tasks_by_kind` (M0.2), `db_pool_connections` (M3.1).
4. **`worker_loop_last_iteration_timestamp_seconds{loop=…}`** (gauge) — heartbeat de
   chaque boucle. Alerte : `time() - heartbeat > 60` = boucle morte. La readiness probe
   ne couvre que HTTP+DB, pas les workers.

## Lot M4 — Métier / batch & claim (opportuniste)

> Confort d'exploitation plus que prévention d'incident — à faire quand l'occasion se
> présente, ou sur besoin réel.

1. **`tasks_deduped_total`** (counter) — tâches skippées par `dedupe_strategy` dans
   `insert_task_batch`. Aujourd'hui indiscernable de « pas de trafic ».
2. **`batch_insert_tasks`** (histogram) — taille des batches `POST /task` ;
   dimensionne les buckets avec la réalité avant d'optimiser plus loin.
3. **`batches_completed_total`** (counter) — signaux `batch_complete` enqueueés
   (`maybe_enqueue_batch_complete`).
4. **`claim_pages_scanned`** (histogram) — pages keyset parcourues par itération de
   `claim_phase`. Révèle un backlog massif bloqué par des règles (le cas qui
   déclencherait le cache des Capacity keys noté « hors scope » au Lot 1).
5. **`concurrency_ko_cache_hits_total`** (counter) — efficacité du cache `ko` de la
   boucle de claim.

---

## Garde-fous transverses

- **Cardinalité** : jamais de `task_id`/`batch_id` en label. `kind` est borné par
  l'usage client — acceptable (déjà le cas sur `tasks_completed_total`), à surveiller.
- **Coût** : aucune métrique de ce plan n'ajoute de requête DB dans un chemin HTTP.
  Les jauges DB (M2.1, M2.2, M3.3) vivent dans les boucles de fond, à leur cadence.
- **Breaking changes** : M0.1 et M0.4 cassent des dashboards existants — les grouper
  dans un seul déploiement annoncé.
- **Tests** : les helpers de `src/metrics.rs` restent des fonctions triviales non
  testées ; tester plutôt *l'appel* des helpers dans les tests d'intégration existants
  quand c'est bon marché (ex. asserter `webhook_mark_failures_total` dans un test de
  mark en échec — sinon s'abstenir, la valeur est dans le câblage, pas la logique).
- **Process** : lots assez petits pour être faits en direct (pas besoin d'agent) ;
  M0+M1 ensemble font une session courte. Ordre recommandé : M0 → M1 → M2 → M3, M4
  au fil de l'eau.
