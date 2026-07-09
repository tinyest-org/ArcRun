# Handoff — État courant et reprise

Contexte de reprise pour la prochaine session.

## Campagne en cours : fixes Audit 2 (depuis 2026-07-08)

**Avancement au 2026-07-09** : **Lot 4 terminé** — 4.1 (c7cfe58), 4.2 (e0c6867),
4.3 (3648456), 4.4 (95ee8b4), 4.5 (3887fab), 4.6 (1f58c49), 4.7 (4f62c15),
4.8 (pré-lock ordonné A9) faits. Suivant : Lot 5 (sécurité), premier item 5.1
(A5, SSRF). Détail et notes de relecture par item dans
`docs/audits/AUDIT_2_FIXES.md`. Décisions utilisateur actées : pause = Pending+Waiting,
Running → 400 (4.5) ; doc metadata = replace, merge → Lot 7 (4.7). Renversement
assumé du commit 2e50620 en 4.4 (compteurs terminaux gelés).

- **Référence d'analyse** : `docs/audits/AUDIT_2_CLAUDE.md` — audit complet perf,
  correctness et architecture (post Lots 0-3). Chaque finding : fichier:ligne, scénario
  d'échec, fix suggéré, sévérité. Findings majeurs contre-vérifiés à la main.
- **Suivi d'exécution** : `docs/audits/AUDIT_2_FIXES.md` — tableau des items (Lots 4-7),
  process de la boucle, prompt de reprise, journal. **C'est là qu'on lit l'état et
  qu'on le met à jour.**
- **Rôles** : implémentation déléguée à un agent **Opus 4.8** (Agent tool, `model: opus`)
  avec prompt détaillé ; **relecture indépendante par Fable** (session principale) :
  diff complet, invariants, re-run `cargo check` + suites ciblées + régressions
  (`tests/integration/test_bug_audit3.rs`). Ne pas se fier au rapport de l'implémenteur.
- Ordre : Lot 4 (correctness critique) → Lot 5 (sécurité) → Lot 6 (perf non-breaking).
  Le Lot 7 (breaking : API v1, batch.remaining, rule_slot, GET /work) ne se lance que
  sur décision explicite de l'utilisateur.

## Historique

- **Plan perf & correctness (Lots 0-3) : TERMINÉ** (2026-06-12, committé). Référence de
  design et décisions actées : `docs/perf-correctness-plan.md` — à lire avant de toucher
  claim loop / outbox / insertion groupée / batch-complete, pour ne pas régresser une
  décision déjà tranchée (LIMIT-less visibility, on_start synchrone = control-flow,
  pas de CTE récursive, enqueue outbox inconditionnel…).
- Audit 1 (2026-02-14) : `docs/audits/AUDIT_1_*.md`, régressions dans
  `tests/integration/test_bug_audit{1,2}.rs`.

## Process de travail (à reconduire)

1. L'implémentation est déléguée à un agent Opus (prompt : section du plan/audit copiée,
   fichiers, invariants CLAUDE.md, tests exigés).
2. La session principale fait la relecture indépendante (diff complet, invariants,
   re-run des tests — jamais sur la seule foi du rapport de l'agent).
3. Mise à jour du tableau de suivi + commit à chaque item (staging explicite).

## Pièges d'environnement connus

- **Tests d'intégration** : cible unique — `cargo test --test integration <filtre>`
  (ex. `outbox`, `claim_loop`, `priority`). Les fichiers sont des modules déclarés dans
  `tests/integration/main.rs`. Testcontainers exige Docker et l'image `postgres:18-alpine`
  (la pré-puller si le premier run timeout : un timeout de création de conteneur
  empoisonne le `LazyLock` partagé et fait échouer toute la suite).
- **Docker Desktop fragile** : un disque plein l'a fait crasher en laissant un
  `com.docker.backend` zombie qui survit au SIGTERM et fait croire à `open -a Docker`
  que tout tourne. Remède : `kill -9 <pid backend>` puis `open -a Docker`.
- **Espace disque** : les builds cargo remplissent vite le disque. En cas de
  `No space left on device` : `rm -rf ~/.cargo/target/debug/incremental`, ou
  `cargo clean` si insuffisant (rebuild complet ensuite).
- **Working tree partagé** : une autre session peut travailler sur `ui/`. Toujours
  **stager explicitement** les fichiers de son lot, jamais `git add -A`. Ne jamais
  toucher `static/dag.html` ni `ui/`.
- **Commits** : pas de trailer Co-Authored-By.
