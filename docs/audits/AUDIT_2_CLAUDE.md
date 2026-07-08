# Audit 2 — Performance, Correctness & Architecture

Date : 2026-07-08
Périmètre : backend complet (`src/`), post plan perf & correctness (Lots 0–3, cf. `docs/perf-correctness-plan.md`).
Méthode : 3 audits parallèles indépendants (couche DB/SQL, workers, API/architecture) + contre-vérification manuelle des findings majeurs dans le code. Les findings marqués **[vérifié]** ont été relus à la main dans les sources ; les décisions actées du plan (outbox, claim paginé, on_start synchrone, pas de CTE récursive) ne sont pas remises en cause.

Ce qui a été vérifié comme **déjà correct** (non re-signalé) : race timeout-vs-PATCH, write-skew batch-complete (verrou `FOR UPDATE`), double-claim outbox (lease), gate start-before-end pour les lignes batch, head-of-line blocking du claim, races de décrément entre deux parents, double-cascade des diamants, orphan-sweep vs signal pending.

---

## A. CORRECTNESS — bugs par sévérité

### A1 — `run_in_transaction` : BEGIN/COMMIT bruts, non-sûr à l'annulation — **CRITIQUE** [vérifié]
`src/db/mod.rs:55-74` émet `sql_query("BEGIN") … COMMIT/ROLLBACK`, en contournant l'`AnsiTransactionManager` de diesel-async. Le `is_broken` de bb8 n'inspecte que l'état du manager — jamais mis à jour ici.
**Scénario** : actix droppe la future d'un handler (déconnexion client, timeout) entre `BEGIN` et `COMMIT` (p.ex. au milieu d'`update_running_task` ou de `cancel_task`). La connexion revient au pool **avec une transaction ouverte tenant des row locks** (dont le `FOR UPDATE` batch). L'emprunteur suivant exécute ses statements « autocommit » *dans* cette transaction ; un `COMMIT` ultérieur rend durable une transition à moitié propagée. Viole « API response = durable state » dans l'autre sens.
**Fix (non-breaking, un seul point)** : remplacer par `conn.transaction(|conn| …)` de diesel-async (même forme de closure `BoxFuture`) — le manager marque la connexion broken en cas d'annulation mi-tx et bb8 la jette.

### A2 — Gate start-before-end : blocage permanent de la livraison end/cancel — **HIGH**
Deux chemins laissent une ligne outbox `start` en `pending` pour toujours, et le gate (`src/db/webhook_execution.rs:432-437`) retient alors les lignes `end`/`cancel` de la tâche **indéfiniment** :
1. Crash (ou simple erreur d'UPDATE, loggée et avalée, `start_loop.rs:391-400`) entre `mark_task_running` et `complete_webhook_execution` : la tâche est `Running`, rien ne re-exécute jamais son start (le requeue ne touche que `Claimed`).
2. Tâche `Claimed` annulée/stop_batchée/dead-end-annulée après un crash mi-webhook, jamais re-claimée.
Effet secondaire : ces lignes mûres-mais-injouables comptent dans `outbox_backlog_stats` → `oldest_ready_age_secs` croît sans borne (fausse alerte permanente).
**Fix** : compléter la ligne start et `mark_task_running` dans la même transaction ; et borner le gate par fraîcheur (`s.updated_at > now() - claim_timeout`), miroir du `stale_after` existant.

### A3 — `Paused` est un état-piège — **HIGH** [vérifié]
Trois défauts qui se combinent :
- **Aucun chemin de resume n'existe.** La doc de l'endpoint (`handlers/task.rs:370`) dit « PATCH vers Pending », mais `validate_update_task` n'accepte que `Success|Failure` et `update_running_task` ne matche que `Running|Claimed`. Seule sortie : cancel/stop_batch.
- **Pause d'une tâche `Waiting`** : toute la propagation filtre `status = Waiting` (`propagation.rs:83-157`) → les décréments `wait_*` et les cascade-fail des parents terminés pendant la pause sont perdus à jamais ; compteurs irrattrapables, batch jamais terminal, `on_batch_complete` jamais tiré.
- **Pause d'une tâche `Running`** : elle échappe à la timeout loop (ne scanne que Running) et le PATCH du worker externe fait 404 — tâche coincée.
**Fix** : restreindre `pause` (interdire Waiting, décider pour Running), inclure `Paused` dans les décréments/cascades, ajouter un endpoint resume contextuel (`wait_* > 0 ? Waiting : Pending`).

### A4 — Fenêtre `Claimed` + on_start en vol : travail zombie chez le consommateur — **HIGH** [vérifié]
- `save_cancel_actions` n'est appelé que dans la branche `Ok(true)` de `mark_task_running` (`start_loop.rs:353-388`) : si la tâche a quitté `Claimed` pendant le HTTP (PATCH, cancel, stop_batch, requeue), l'action cancel retournée par le consommateur est **silencieusement droppée**.
- `cancel_task` n'enqueue le webhook cancel que si `status == Running` (`propagation.rs:336`) et `stop_batch` suppose « Claimed = on_start pas encore appelé » (`task_lifecycle.rs:380`) — faux : `Claimed` couvre toute la fenêtre webhook-en-vol (file de permits + jusqu'à 10 s/action).
**Conséquence** : le consommateur a reçu on_start, démarre le travail, ne recevra jamais de cancel. **Fix** : sauver les cancel actions quel que soit le résultat de `mark_task_running` ; enqueuer le cancel outbox aussi pour `Claimed` (sûr : le gate retient la ligne tant que le start est pending, et zéro-action ⇒ fast-path success).

### A5 — SSRF : deux contournements réels — **HIGH (prod/release)**
- **IPv6 littéral** : `url.host_str()` renvoie l'hôte **avec crochets** (`"[::1]"`), donc `host.parse::<IpAddr>()` (`ssrf.rs:57`) échoue toujours et `is_internal_ip` n'est jamais consulté. `http://[::1]:8085/`, `http://[fd00::1]/`, `http://[::ffff:10.0.0.1]/` passent la validation en release. Fix : `url.host()` (renvoie `Host::Ipv6` parsé) + gérer `to_ipv4_mapped()`.
- **DNS rebinding** : la validation ne résout jamais le nom ; reqwest résout à la livraison (potentiellement des heures plus tard, retries). Enregistrer un domaine → IP publique à la création, flipper vers `169.254.169.254` avant livraison. Les redirects sont déjà désactivés (bien) ; le rebinding est ouvert. Fix : resolver custom reqwest qui vérifie l'IP résolue au moment de la requête.

### A6 — Aucune authentification — **HIGH (rayon d'impact)**
Seul middleware : Prometheus. Tout endpoint est ouvert : création de tâches avec webhooks sortants arbitraires (= lanceur SSRF/relais DoS même avec A5 corrigé), cancel/stop de n'importe quel batch, lecture de toutes les métadonnées, `/metrics`, Swagger. Le `?handle=` est une capability URL nue (l'UUID est le seul secret, et il transite en query string → logs d'accès des consommateurs). Fix minimal : bearer token statique + `/health`,`/ready` ouverts ; handle en header ou signé HMAC.

### A7 — Pipeline de compteurs PUT : mutation de tâches terminales + poison pill — **MED/HIGH** [vérifié]
- Le flush UNNEST (`batch_updater.rs:148-160`) n'a **aucun garde de statut** : un flush qui atterrit après le PATCH terminal mute `success`/`failures`/`last_updated` d'une tâche terminale (viole « états terminaux immuables », divergence avec les counts déjà notifiés) et repousse le requeue des `Claimed`.
- **Poison pill** : le flush est un seul statement multi-lignes ; en erreur, tout est re-queued sans backoff ni cap. Une ligne qui fait systématiquement erreur (cas réaliste : overflow `i32` de `success + s`, rien ne borne les compteurs) bloque **définitivement** tous les compteurs de toutes les tâches, jusqu'au restart (qui perd la DashMap).
**Fix** : `AND task.status IN ('running','claimed')` dans le flush ; en cas d'échec batch, bisecter/retomber en per-row et dropper+logger les lignes déterministiquement fautives ; `LEAST(success + s, 2147483647)` en SQL.

### A8 — Dedupe inter-requêtes : check-then-act sans contrainte — **MED**
`handle_dedupe` (`task_crud.rs:16-66`) = `COUNT(*)` sur le snapshot committé puis INSERT plus tard dans la tx. Deux `POST /task` concurrents avec la même clé dedupe passent tous les deux → doublons malgré `dedupe_strategy`. Fix : `pg_advisory_xact_lock(hash(kind, status, fields))` avant le count (l'infra de hash existe déjà dans `rule::compute_lock_key`).

### A9 — Deadlocks potentiels sur les UPDATE multi-lignes — **MED/LOW**
`UPDATE task … WHERE id = ANY($ids)` (propagation, batch-claim, flush compteurs) verrouille dans l'ordre du plan, pas de l'array. Deux transitions concurrentes de parents partageant des enfants (diamants), ou un flush vs une propagation, peuvent se croiser → 40P01 → 500 sur le PATCH. Fix : `SELECT … ORDER BY id FOR UPDATE` avant les UPDATE batchés, ou retry sur 40P01.

### A10 — Correctness divers (MED/LOW)
- **Cancel d'une tâche `Waiting` impossible** (`propagation.rs:298-308`) alors que la doc de l'endpoint l'annonce — impossible d'élaguer une branche de DAG non éligible.
- **DELETE/pause : toute erreur → 400 vide** (`handlers/task.rs:361,385`) — une panne DB devient une erreur client, indistinguable d'un mauvais id.
- **PATCH jamais 409, non-idempotent** : 0-row → 404 générique ; un retry client après réponse perdue ne peut pas distinguer « déjà appliqué » de « mauvais id ». Fix : SELECT de suivi → 404 / 200 idempotent / 409 avec statut courant.
- **Filtre `metadata` invalide ignoré silencieusement** (`dtos/query.rs:115`) : `GET /task?metadata={malformé}` renvoie TOUTES les tâches. Fix : 400 comme le filtre status.
- **Pas de limites structurelles sur POST /task** : ni nb de tâches/batch, ni deps/tâche, ni actions/tâche ; seul backstop = limite Json actix implicite (2 Mo, non configurée). ~5 000 tâches dedupe-free dépassent les 65 535 bind params de Postgres → 500. Le DFS de détection de cycles est récursif → ~15 k tâches chaînées = stack overflow plausible (DoS non authentifié). NB : la règle « une dépendance référence une tâche définie avant » exclut déjà les cycles par construction — le DFS est redondant.
- **Circuit breaker quasi inopérant** : ne couvre que l'acquisition pool côté handlers ; un échec n'est compté qu'après ~90 s de retries vs fenêtre de comptage de 10 s (ne peut quasiment jamais tripper séquentiellement) ; HalfOpen admet toutes les requêtes (pas « one probe »).
- **PATCH `metadata` documenté « merged » mais fait un remplacement complet** (`task_lifecycle.rs:67`) — un client partial-update efface les champs utilisés par les matchers de règles/dedupe.
- **Rétention** : le cleanup supprime les lignes `webhook_execution` encore `pending` d'une tâche hors rétention (at-least-once devient at-most-retention) ; les counts d'un `batch_complete` longuement retryé peuvent sous-compter.
- **Shutdown** : la snapshot du final-flush peut racer le receiver (events dans le canal perdus) ; flush final per-row sous timeout de 10 s ; une itération webhook_phase peut dépasser le join timeout → on_start tué en vol (acceptable at-least-once, mais churn au redeploy).
- **Sémantique cancel incohérente** : cancel manuel Running → `cancel` seul ; ancêtre dead-end → `cancel` + `end/failure` ; un `on_failure` consommateur tire pour une saveur d'annulation mais pas les autres.

---

## B. PERFORMANCE — améliorations non-breaking

### B1 — on_start tient une connexion DB pendant tout le HTTP — **HIGH** [vérifié]
`execute_webhook_for_task` (`start_loop.rs:337`) acquiert la connexion **avant** `start_task` (qui fait les appels HTTP, jusqu'à 10 s chacun). Défauts : `WORKER_WEBHOOK_CONCURRENCY = 10 = POOL_MAX_SIZE` (la validation ne prévient que si `>`, pas `==`) → un lot d'on_start lents épuise **tout le pool** et affame handlers + les 5 autres loops. C'est exactement la pathologie que le Lot 2 a éliminée pour les end/cancel. **Fix** : même découpage en phases que la delivery loop (conn pour try_claim + load actions, drop, HTTP, ré-acquisition pour mark_running).

### B2 — Plafond de débit on_start (quantifié)
Débit ≈ `min(claim_cap/itération, concurrency/latence_webhook)`. Défauts (cap 50, concurrency 10, intervalle 1 s) : latence 100 ms → ~30-35 tâches/s ; 500 ms → ~14/s ; 2 s → ~4,5/s ; 10 s → ~1/s **et** la file de permits dépasse `claim_timeout` (jusqu'à 40 s d'attente vs 30 s) → churn de requeue. **Fix respectant la décision actée (couplage = backpressure)** : heartbeater les claims en file d'attente (bump `last_updated` en attendant un permit), puis monter concurrency/cap sans risque, et pipeliner claim de la page suivante pendant les webhooks.

### B3 — Détection batch-complete : O(N) par transition sous verrou ⇒ O(N²) par batch — **HIGH à l'échelle**
Le `NOT EXISTS (task WHERE batch_id=$1 AND status NOT IN …)` tourne à **chaque** transition terminale, sérialisé par le verrou batch ; seul `idx_task_batch_id` existe → en fin de vie d'un gros batch, le probe visite presque toutes les lignes terminales. Batch 50 k ≈ 1,25 Md de visites de lignes cumulées.
**Fix immédiat** : `CREATE INDEX idx_task_batch_active ON task(batch_id) WHERE status NOT IN ('success','failure','canceled')` — les statuts sont des littéraux dans ce SQL brut, l'index partiel qualifie, le probe devient O(1).
**Bonus** : le `FOR UPDATE` verrouille aussi les batches scope/metadata-only (`on_complete = []`) — toutes leurs transitions terminales se sérialisent pour rien depuis #601. Mettre le prédicat de non-vacuité dans le statement verrouillant.

### B4 — Latence plancher du polling — **MED**
Coût DB idle trivial (~5-6 stmts/s). Le vrai coût : chaque arête de DAG paie ~0,5 s (tick claim) + ≤1 s (tick delivery) → un pipeline de profondeur 20 en tâches instantanées passe ~20-40 s en pur scheduling. **Fix** : `tokio::sync::Notify` in-process — les handlers nudgent la start loop après commit, les transitions nudgent la delivery loop — poll 1 s conservé en réconciliation. (LISTEN/NOTIFY seulement si multi-réplica, et comme optimisation, jamais comme correctness.)

### B5 — Cascade d'échec O(nœuds) séquentielle dans la tx du PATCH — **MED**
La récursion de `propagate_to_children` est par-enfant-échoué, pas par-niveau (`propagation.rs:109-113`) — le « déjà batché par niveau » du plan vaut pour les décréments, pas la cascade. Un échec racine sur 1 000 descendants ≈ 1 000+ SELECT/UPDATE séquentiels dans la transaction du PATCH, locks tenus, latence en secondes. **Fix** (compatible décision « pas de CTE récursive », reste en Rust) : frontière par niveau — un `eq_any` UPDATE par niveau, récursion sur le set retourné : O(profondeur) au lieu de O(nœuds).

### B6 — Requêtes/endpoints lourds
- **`GET /batches`** : la CTE agrège **toute** la table task (GROUP BY + ILIKE sur metadata::text) avant LIMIT/OFFSET — O(total tasks) par page. Paginer d'abord sur `batch`, agréger seulement la page.
- **POST /task écho** : renvoie des `TaskDto` complets (metadata + rules + toutes les actions) ≈ taille de la requête en retour, et l'annotation OpenAPI promet `Vec<BasicTaskDto>` — renvoyer BasicTaskDto (c'est un fix de conformité au schéma publié) et dropper le `RETURNING` des actions.
- **`GET /dag/{batch_id}`** non borné (batch 15 k = réponse multi-Mo).
- **`GET /webhook-deliveries`** : tri `updated_at DESC` sans index → seq scan+sort ; remplacer `idx_webhook_execution_status` par `(status, updated_at DESC)`.
- **Pagination OFFSET sans tiebreaker unique** : `created_at DESC` seul (les inserts groupés créent des created_at identiques !) → pages qui répètent/sautent des lignes. Ajouter `id` en tiebreaker partout (pas cher, tout de suite).
- **Untagged `CreateTaskBody`** : ~2× le coût de parse du plus gros payload de l'API + messages d'erreur détruits (« did not match any variant »). Fix partiel non-breaking : dispatch manuel sur le premier octet `[` vs `{`.

### B7 — Hygiène index & schéma (gains d'écriture)
- Index morts/redondants sur les tables les plus chaudes : `idx_action_task_id` (préfixe de `idx_action_task_id_trigger`), `idx_action_trigger` (aucune requête), `idx_task_kind` (prédicats kind = LIKE '%…%' inindexables) — vérifier `pg_stat_user_indexes` puis dropper.
- `idx_task_priority` sans `id` final → incremental sort à chaque page de claim ; ajouter `id`.
- JSONB over-fetch : listings et `get_dag_for_batch` chargent metadata + start_condition (64 Ko max chacun) pour produire des `BasicTaskDto` qui n'en contiennent aucun ; `requeue_stale_claimed_tasks` RETURNING des lignes complètes.
- **UUIDv7** : les UUID sont générés côté app depuis le Lot 3a → passer `Uuid::new_v4()` → `now_v7()` est un one-liner **sans migration** ; localité d'insertion B-tree sur le PK et toutes les structures `(…, id)`. (Le commentaire de la 1re migration le prévoyait déjà.)
- **Rétention désactivée par défaut** (`RETENTION_ENABLED=0`) alors que tout le design la suppose (enqueue outbox inconditionnel, lignes `success` gardées, index all-status). Croissance non bornée par défaut → flipper le défaut ou warning bruyant au démarrage.
- ko-cache : cardinalité par (règle, valeurs metadata) — backlog multi-tenant (5 000 projets bloqués) ≈ 20 k round-trips/s. Négative-cache TTL inter-itérations + invalidation via le canal de wake-up (B4).
- timeout_loop sans LIMIT et séquentielle (mass-timeout de 10 k tâches = loop bloquée des minutes, requeue des Claimed retardé car même loop) ; probes health bloquants jusqu'à 30 s sous pool exhaustion (restart storm kubelet).

---

## C. ARCHITECTURE — évaluation

**Le design 5-loops + polling est le bon socle.** Chaque transition est un statement gardé ou une tx SKIP LOCKED ; l'état est la vérité ; les sweeps sont auto-réparants (crash n'importe où → une loop répare). **Ne pas le remplacer — l'augmenter** : wake-ups événementiels (B4) par-dessus, polls conservés en réconciliation. Un redesign purement event-driven troquerait l'auto-réparation contre une latence qu'on obtient pour ~30 lignes de `Notify`.

**La soundness du claim ne vit pas dans le scan** mais dans `claim_task_with_rules` (advisory xact locks + count-and-claim en un statement/snapshot). Le scan n'est qu'un préfiltre. Deux trous réels, qui ne mordent **qu'en multi-réplica** :
1. Les tâches sans règle sont *comptées* par les règles des autres mais claimées **sans verrou** (`batch_claim_tasks`) — deux réplicas peuvent dépasser un max_concurency.
2. Deux règles à matchers chevauchants mais clés de lock différentes (`fields:["projectId"]` vs `fields:[]`) ne se voient pas.

**Scale horizontal aujourd'hui : presque.** Replica-safe : outbox (lease), timeout (SKIP LOCKED), requeue, compteurs batch_updater (additifs), claims simples. Bloqueurs : les 2 trous ci-dessus, la fenêtre `stale_after` (30 s) < durée max d'une séquence on_start (N×10 s) → double on_start inter-réplicas, gauges métriques échantillonnées par instance (double-comptage Prometheus).

---

## D. BREAKING CHANGES — « peut-on augmenter les perfos ? » → oui, nettement

Par ordre de valeur :

### D1 — Table `rule_slot` : règles de concurrence enforced par la DB
`rule_slot(lock_key PK, used, max)` ; le claim fait `UPDATE rule_slot SET used = used+1 WHERE lock_key = ANY(…) AND used < max` dans sa tx ; les transitions terminales décrémentent dans la leur (mêmes 6 call sites que `maybe_enqueue_batch_complete` — le pattern de centralisation existe déjà). O(1) par claim (plus de COUNT sur `task`), replica-safe par row locking, supprime la couche advisory locks et l'essentiel du problème ko-cache. Les Capacity rules demandent de pousser les deltas `remaining` depuis le flush du batch_updater (plus invasif). **Prérequis du scale horizontal ; pas rentable en mono-process.**

### D2 — `batch.remaining` : compteur au lieu de FOR UPDATE + NOT EXISTS
`UPDATE batch SET remaining = remaining - 1 … RETURNING remaining` dans chaque transition : atomique, O(1), sérialisation naturelle sur la ligne, `remaining = 0` **est** le signal de complétion — remplace B3 proprement et donne le progress reporting gratuit. Attention : dedupe-skipped (connus à l'insert) et `stop_batch` (set 0).

### D3 — Séparer l'outbox du ledger d'idempotence
`webhook_execution` est à la fois queue at-least-once, ledger d'idempotence on_start et log d'observabilité. La queue veut des lignes supprimées au succès (petite, chaude, vacuum-friendly) ; le ledger veut de la rétention. Aujourd'hui chaque notification livrée vit pour toujours en `success` (rétention off par défaut). Soit DELETE des lignes end/cancel au succès (le gate ne lit que les lignes `start`), soit table `webhook_outbox` dédiée. Se marie avec l'extraction de la delivery loop en **binaire dispatcher séparé** (même crate, même DB — quasi gratuit : elle ne partage rien in-process ; isole la pression CPU/sockets des downstreams lents, scalable indépendamment, le lease est déjà replica-safe). `on_start` reste dans l'orchestrateur (décision actée).

### D4 — API v1 (breaking « produit »)
- **Enveloppe de réponse + versionnement** : le 201 de POST /task est un tableau nu → impossible d'ajouter des champs batch-level. Passer à `/v1` + `{batch_id, tasks, deduped}` (aujourd'hui le nombre de dédupliqués est invisible).
- **POST /task idempotent** : batch_id généré serveur → un retry client après 201 perdu crée un **DAG dupliqué**. Accepter `Idempotency-Key` (ou batch UUID client), unique-indexé sur `batch`, replay de la réponse au conflit. On donne déjà des clés d'idempotence aux consommateurs — en donner une aux producteurs.
- **Démêler PATCH/PUT en verbes** : `POST /task/{id}/complete|progress|cancel|pause|resume` — un DTO chacun, plus de `UpdateTaskDto` partagé avec des champs ignorés par endpoint.
- **Retirer le body bare-array** (déprécier puis supprimer) → tue l'enum untagged (B6) ; la forme objet est strictement plus capable.
- **`?handle=` signé** : garder le pattern (bonne primitive de réconciliation) mais header + HMAC avec expiration — une URL fuitée des access logs du consommateur ne doit pas être une capability d'écriture.
- **Dépendances : rester intra-batch** (l'invariant #7 est porteur : la sûreté de stop_batch en dépend). Si besoin de coordination inter-batch : une dépendance `{"batch": "<uuid>"}` résolue comme « signal batch-complete de ce batch » réutilise la détection existante — bien moins cher que des links inter-batch.

### D5 — API pull-based `GET /work` en **complément** (pas en remplacement)
`POST /work/claim {kinds, max, lease_secs}` + `POST /work/{id}/heartbeat|complete`. Mappe quasi 1:1 sur l'existant : statut `Claimed`, `claim_task_with_rules`, `requeue_stale_claimed_tasks` est déjà une expiration de lease, le keepalive `last_updated` est déjà le heartbeat. Bénéfices : les workers derrière NAT sans endpoint HTTP exposé, **toute la classe SSRF (A5) n'existe que parce que le push est obligatoire**, `on_start` devient optionnel (aujourd'hui obligatoire dans `NewTaskDto` — contrainte étrange), et le plafond B2 disparaît pour les workers pull (plus de webhook synchrone du tout).

### D6 — Croissance de la table task
- Ne **pas** partitionner par statut (chaque transition déplacerait la ligne de partition).
- Si des millions de lignes terminales sont l'état stable : table `task_archive` balayée par la rétention (garde la table chaude petite, les index all-status serrés, et préserve l'historique de `GET /task/{id}` mieux que le DELETE) ; le range-partitioning par `created_at` (rétention = DROP PARTITION, zéro dette vacuum) est l'étape d'après, au prix du rework PK/FK.

### D7 — Bundle multi-réplica
D1 + labels métriques par instance + jitter ou leader-lease sur la start_loop + `stale_after ≥` durée max d'une séquence on_start.

---

## E. Chiffrage honnête des gains

| Axe | Aujourd'hui | Après fixes A/B (non-breaking) | Après D (breaking) |
|---|---|---|---|
| Débit de démarrage de tâches | ~30/s (webhooks 100 ms), ~1/s (aval lent), pool entier gelable | pool jamais gelé (B1), concurrency montable ×5-10 avec heartbeat (B2) | plafond supprimé pour les workers pull (D5) |
| Latence de scheduling par arête de DAG | ~0,5-1,5 s | ~10 ms (Notify, B4) | idem + inter-process via LISTEN/NOTIFY |
| Batch 50 k tâches | O(N²) en fin de vie, transitions sérialisées | O(N) (index partiel B3) | O(N) avec compteur (D2), progress gratuit |
| PATCH avec grosse cascade | O(nœuds) requêtes, secondes | O(profondeur) (B5) | idem |
| Scale horizontal | non (règles) | non | oui (D1 + D7) |
| Croissance stockage | non bornée par défaut | bornée (rétention on) | plate (D3 + D6) |

## F. Ordre d'attaque recommandé (format Lots, process habituel)

1. **Lot 4 — Correctness critique** : A1 (transaction manager), A2 (gate + complete-in-tx), A4 (fenêtre Claimed), A7 (garde statut + anti-poison), A3 (Paused). Petits diffs, tests de régression `test_bug_audit3.rs`.
2. **Lot 5 — Sécurité** : A5 (IPv6 + rebinding), A6 (auth minimale), limites structurelles POST /task (A10).
3. **Lot 6 — Perf non-breaking** : B1 (phase-split on_start), B3 (index partiel + prédicat lock), B5 (cascade par niveau), B4 (Notify), B6/B7 (écho POST, tiebreakers, index morts, UUIDv7, rétention on).
4. **Lot 7 — Breaking (si fenêtre)** : D4 (API v1 + idempotence) + D2 (`batch.remaining`) ; D1/D5/D7 quand le multi-réplica ou les workers pull deviennent un besoin réel.
