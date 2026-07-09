# Handoff — État courant et reprise

Contexte de reprise pour la prochaine session.

## Campagne en cours : fixes Audit 2 (depuis 2026-07-08)

**Avancement au 2026-07-09** : **Lots 4 et 5 terminés** — Lot 4 (4.1 → 4.8,
dernier commit 01d034f) ; Lot 5 : 5.1 (A5 SSRF, 36b6f7a), 5.2 (A6 auth,
c3bee32), 5.3 (A10 limites, eaf67f6). **Lot 6 en cours** : 6.1 (B1, phases
A/B/C, 1623934), 6.2 (B3, index partiel + lock batch, bd64090), 6.3 (B5,
cascade par frontière — bug de multiplicité des décréments attrapé en
relecture) faits ; suivant 6.4 (B4, wake-ups `tokio::sync::Notify`), puis
6.5 (B2, après 6.1) et 6.6 (B6/B7, découpable). Détail et notes de
relecture par item dans `docs/audits/AUDIT_2_FIXES.md`. Décisions actées : pause = Pending+Waiting,
Running → 400 (4.5) ; doc metadata = replace, merge → Lot 7 (4.7) ; renversement
assumé du commit 2e50620 en 4.4 (compteurs terminaux gelés) ; HMAC du `?handle=`
→ Lot 7 (5.2) ; auth non-breaking (token absent ⇒ ouvert + warn release, 5.2) ;
limites 5.3 = 1000 tâches/batch, 100 deps/tâche, 20 actions/tâche, payload 2 MiB
(env-overridables) ; DFS de cycles supprimé (forward-reference rule prouvée).

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
