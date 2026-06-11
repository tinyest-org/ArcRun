# Handoff — Lot 2 (outbox transactionnel)

Contexte de reprise pour la prochaine session. La référence de design est
`docs/perf-correctness-plan.md` — **lire ce fichier en premier**, il contient le contrat
API cible, les décisions actées avec leurs raisons, et le design complet du Lot 2.

## État au 2026-06-11

- **Lots 0 et 1 : faits, testés, committés** (commit « perf: lot 0 quick wins + lot 1
  paginated claim loop »). Validation : 156 tests d'intégration + 30 unitaires verts,
  vérifiés indépendamment de l'agent d'implémentation.
- **Prochain : Lot 2 — outbox transactionnel** (section dédiée du plan). Rappel des points
  qui ne doivent pas se perdre :
  - `on_start` ne passe PAS par l'outbox (control-flow : sa réponse pilote l'orchestrateur).
    Seuls les webhooks end/cancel deviennent des notifications at-least-once.
  - Ordre de livraison garanti **par tâche** (start avant end), aucun ordre entre tâches.
  - La table `webhook_execution` existe déjà (idempotency_key UNIQUE, status, attempts) ;
    il manque `exhausted`, `next_attempt_at`, `last_error`, l'index partiel, l'insertion
    in-tx et la boucle de livraison (5e worker).
- **Lot 3** : uniquement sur preuve de besoin (voir plan).

## Process de travail utilisé (à reconduire)

1. L'implémentation des lots est déléguée à un agent **Claude Opus 4.8** (Agent tool,
   `model: opus`) avec un prompt détaillé qui pointe vers la section du plan + liste les
   fichiers, les invariants à préserver et les tests exigés.
2. La session principale fait ensuite une **relecture indépendante** : lecture du diff
   complet, vérification des invariants du plan, re-run de `cargo check` et des suites de
   tests (ne pas se fier au seul rapport de l'agent).

## Pièges d'environnement connus

- **Tests d'intégration** : cible unique — `cargo test --test integration <filtre>`
  (ex. `claim_loop`, `priority`). Les fichiers sont des modules déclarés dans
  `tests/integration/main.rs`. Testcontainers exige Docker et l'image `postgres:18-alpine`
  (la pré-puller si le premier run timeout : un timeout de création de conteneur
  empoisonne le `LazyLock` partagé et fait échouer toute la suite).
- **Docker Desktop fragile** : un disque plein l'a fait crasher en laissant un
  `com.docker.backend` zombie qui survit au SIGTERM et fait croire à `open -a Docker`
  que tout tourne. Remède : `kill -9 <pid backend>` puis `open -a Docker`.
- **Espace disque** : les builds cargo remplissent vite le disque (~26 Gi libres au moment
  du handoff). En cas de `No space left on device` :
  `rm -rf ~/.cargo/target/debug/incremental`.
- **Working tree partagé** : une autre session travaille sur `ui/` (Sidebar,
  CommandPalette…). Toujours **stager explicitement** les fichiers de son lot, jamais
  `git add -A`. Ne jamais toucher `static/dag.html` ni `ui/`.

## Suivis mineurs notés en relecture (non bloquants)

- `WORKER_START_BATCH_SIZE <= 0` signifie « claims illimités » (tous les gardes sont
  `claim_cap > 0`). À documenter ou valider dans `config.rs` à l'occasion.
- Flush anticipé du batch-claim : si un worker concurrent claim des tâches du batch,
  l'itération peut sous-claimer légèrement (rattrapé à l'itération suivante). Bénin.
- Migration `2026-06-11-000001_drop_redundant_status_index` : avant déploiement prod,
  valider par `EXPLAIN` que la `timeout_loop` utilise bien `idx_task_priority`.
