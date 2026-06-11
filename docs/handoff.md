# Handoff — Plan perf & correctness

Contexte de reprise pour la prochaine session. La référence de design est
`docs/perf-correctness-plan.md` — **lire ce fichier en premier**, il contient le contrat
API cible, les décisions actées avec leurs raisons, et l'historique des lots.

## État au 2026-06-12

- **Lots 0, 1, 2 et 3 : faits, testés, committés — le plan perf & correctness est
  terminé.** Lot 3 (insertion groupée + webhook `on_batch_complete`) : 206 tests verts
  (30 unitaires + 176 intégration dont 14 dans `test_batch_complete.rs`), validés
  indépendamment de l'agent d'implémentation. La relecture a trouvé et corrigé 2 bugs
  réels (write-skew sur la détection batch-complete ⇒ verrou FOR UPDATE sur la ligne
  `batch` ; orphan-sweep de rétention vs signal `pending` d'un batch vide) — détail et
  tests de régression dans la section Lot 3 du plan.
- **Les 4 suivis de relecture sont soldés (2026-06-12)** — il ne reste rien d'ouvert :
  1. *Delivery loop* : `run_delivery_once` réécrit en 4 phases — claim court avec lease
     (`claim_due_outbox_leased`, env `WEBHOOK_DELIVERY_LEASE_SECS`), prefetch hors-lock,
     HTTP parallèle borné (`WEBHOOK_DELIVERY_CONCURRENCY`), marks autocommit indépendants.
     Détail dans la section Lot 2 du plan ; 4 nouveaux tests dans
     `tests/integration/test_delivery_lease.rs`. La relecture indépendante a corrigé un
     double comptage de la métrique `exhausted` (payload batch malformé).
  2. *Paramètres `_evaluator` morts* : retirés de `update_running_task`,
     `fail_task_and_propagate`, `cancel_task`, `timeout_loop` (+ tous les call sites).
  3. *`WORKER_START_BATCH_SIZE`* : la validation rejette désormais `<= 0` (les négatifs
     signifiaient silencieusement « claims illimités »).
  4. *Migration drop d'index* : validée par `EXPLAIN ANALYZE` sur Postgres 18 seedé —
     aucune requête de la `timeout_loop` ne fait de seq scan (résultat consigné dans le
     plan, section Lot 0). Bon pour la prod.
- Le contrat webhooks (at-least-once, ordre par tâche, `on_start` = control-flow hors
  outbox, signal `on_batch_complete`) est documenté dans `CLAUDE.md` et implémenté.

## Process de travail utilisé (à reconduire)

1. L'implémentation des lots est déléguée à un agent **Claude Opus 4.8** (Agent tool,
   `model: opus`) avec un prompt détaillé qui pointe vers la section du plan + liste les
   fichiers, les invariants à préserver et les tests exigés.
2. La session principale fait ensuite une **relecture indépendante** : lecture du diff
   complet, vérification des invariants du plan, re-run de `cargo check` et des suites de
   tests (ne pas se fier au seul rapport de l'agent).

## Pièges d'environnement connus

- **Tests d'intégration** : cible unique — `cargo test --test integration <filtre>`
  (ex. `outbox`, `claim_loop`, `priority`). Les fichiers sont des modules déclarés dans
  `tests/integration/main.rs`. Testcontainers exige Docker et l'image `postgres:18-alpine`
  (la pré-puller si le premier run timeout : un timeout de création de conteneur
  empoisonne le `LazyLock` partagé et fait échouer toute la suite).
- **Docker Desktop fragile** : un disque plein l'a fait crasher en laissant un
  `com.docker.backend` zombie qui survit au SIGTERM et fait croire à `open -a Docker`
  que tout tourne. Remède : `kill -9 <pid backend>` puis `open -a Docker`.
- **Espace disque** : les builds cargo remplissent vite le disque — il a saturé à 0
  pendant le Lot 3 (les outils ne pouvaient même plus écrire dans /tmp). En cas de
  `No space left on device` : `rm -rf ~/.cargo/target/debug/incremental`, ou
  `cargo clean` si insuffisant (rebuild complet ensuite).
- **Working tree partagé** : une autre session travaille sur `ui/` (Sidebar,
  CommandPalette…). Toujours **stager explicitement** les fichiers de son lot, jamais
  `git add -A`. Ne jamais toucher `static/dag.html` ni `ui/`.

## Suivis mineurs notés en relecture (non bloquants)

- **Débit de la delivery loop** : `run_delivery_once` exécute tout le batch (HTTP compris)
  dans une seule transaction qui tient les locks — livraison séquentielle. Si le débit
  devient un goulot : claim court + livraison hors-tx parallèle (noté dans le plan, Lot 2).
- **Paramètres `_evaluator` morts** : `update_running_task`, `cancel_task`,
  `fail_task_and_propagate`, `timeout_loop` gardent un paramètre `ActionExecutor` inutilisé
  depuis le passage à l'outbox. Nettoyage cosmétique à l'occasion.
- `WORKER_START_BATCH_SIZE <= 0` signifie « claims illimités » (tous les gardes sont
  `claim_cap > 0`). À documenter ou valider dans `config.rs` à l'occasion.
- Migration `2026-06-11-000001_drop_redundant_status_index` : avant déploiement prod,
  valider par `EXPLAIN` que la `timeout_loop` utilise bien `idx_task_priority`.
