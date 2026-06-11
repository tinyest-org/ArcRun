# Handoff — Plan perf & correctness

Contexte de reprise pour la prochaine session. La référence de design est
`docs/perf-correctness-plan.md` — **lire ce fichier en premier**, il contient le contrat
API cible, les décisions actées avec leurs raisons, et l'historique des lots.

## État au 2026-06-11 (soir)

- **Lots 0, 1 et 2 : faits, testés, committés.** Lot 2 (outbox transactionnel) :
  192 tests verts (30 unitaires + 162 intégration dont 6 nouveaux `test_outbox.rs`),
  validés indépendamment de l'agent d'implémentation.
- **Lot 3 : uniquement sur preuve de besoin** (voir plan : insertion de batch groupée,
  webhook `on_batch_complete`).
- Le contrat webhooks (at-least-once, ordre par tâche, `on_start` = control-flow hors
  outbox) est maintenant documenté dans `CLAUDE.md` et implémenté.

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
- **Espace disque** : les builds cargo remplissent vite le disque (~22 Gi libres au moment
  du handoff). En cas de `No space left on device` :
  `rm -rf ~/.cargo/target/debug/incremental`.
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
