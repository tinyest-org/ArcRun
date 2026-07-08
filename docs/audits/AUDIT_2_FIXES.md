# Campagne de fixes — Audit 2 (2026-07-08)

Suivi d'exécution des corrections issues de `docs/audits/AUDIT_2_CLAUDE.md` (la référence :
chaque item y a son analyse détaillée, fichier:ligne, scénario d'échec, fix suggéré).
Ce fichier est la **source de vérité de l'avancement** — le mettre à jour à chaque itération.

## Process (à reconduire à chaque itération)

Rôles : **Opus 4.8 implémente** (Agent tool, `model: opus`), **Fable relit** (session
principale). Ne jamais laisser l'implémenteur s'auto-valider.

1. Prendre le premier item `À faire` du tableau (ordre des lots), le passer `En cours`.
2. Déléguer l'implémentation à un agent Opus avec un prompt qui contient : la section de
   l'audit concernée (copier le texte, ne pas juste pointer), les fichiers à toucher, les
   invariants de `CLAUDE.md` à préserver, les tests exigés, et les contraintes
   d'environnement (ci-dessous).
3. Relecture indépendante par Fable : lire le **diff complet**, vérifier que le scénario
   d'échec de l'audit est réellement fermé, re-runner `cargo check` + les suites ciblées
   (`cargo test --test integration <filtre>`) + les unitaires si touchés. Quand c'est
   faisable, vérifier que la régression échoue avec le fix reverti. Ne pas se fier au
   rapport de l'agent.
4. Mettre à jour le tableau (statut, commit, notes de relecture). Stager **explicitement**
   les fichiers du lot (jamais `git add -A`), committer (pas de trailer Co-Authored-By).
5. **Un commit par itération, obligatoire** : l'item courant est committé (code + tests +
   mise à jour de ce tableau dans le même commit) AVANT de démarrer l'item suivant.
   Jamais deux items dans un même commit, jamais d'item démarré sur un working tree sale.
6. Item suivant si le contexte le permet ; sinon mettre à jour `docs/handoff.md` et
   s'arrêter proprement.

### Tests de régression
Nouveau fichier `tests/integration/test_bug_audit3.rs` (déclaré dans
`tests/integration/main.rs`). Convention : `test_audit2_<id>_<description>` (ex.
`test_audit2_a3_paused_waiting_receives_decrements`), doc comment expliquant le bug
d'origine, le fix, et ce que le test asserte. Helpers partagés de `tests/integration/common/`.

### Contraintes d'environnement (rappel, détail dans docs/handoff.md)
- Tests d'intégration : `cargo test --test integration <filtre>` ; Docker + image
  `postgres:18-alpine` requis (pré-puller ; un timeout empoisonne le LazyLock partagé).
- Ne jamais toucher `ui/` ni `static/dag.html` ; disque : purger
  `~/.cargo/target/debug/incremental` si saturation.
- Migrations : toujours fournir un `down.sql` fonctionnel.

## Prompt de boucle (à coller pour reprendre la campagne)

```
Reprise de la campagne de fixes Audit 2 d'ArcRun.
Lis dans l'ordre : docs/handoff.md, docs/audits/AUDIT_2_FIXES.md (process + état),
puis la section de docs/audits/AUDIT_2_CLAUDE.md correspondant à l'item à traiter.

Exécute la boucle : prends le premier item "À faire" du tableau (ordre des lots),
passe-le "En cours", délègue l'implémentation à un agent Opus (Agent tool,
model: opus) avec un prompt détaillé (texte de l'audit, fichiers, invariants
CLAUDE.md, tests exigés dans tests/integration/test_bug_audit3.rs, contraintes
d'environnement). À son retour, fais la relecture indépendante toi-même : diff
complet, scénario d'échec fermé, cargo check, suites ciblées, nouvelle régression
verte (et rouge si fix reverti quand faisable). Corrige ou refais faire ce qui ne
passe pas. Puis COMMITE l'itération : un commit par item (code + tests + mise à jour
du tableau AUDIT_2_FIXES.md dans le même commit), staging explicite (jamais
git add -A), pas de trailer Co-Authored-By. Ne démarre JAMAIS l'item suivant avant
que le précédent soit committé et le working tree propre.
Enchaîne sur l'item suivant tant que le contexte le permet ; avant de t'arrêter,
mets à jour docs/handoff.md avec l'état exact.
```

## Suivi

Statuts : `À faire` / `En cours` / `Fait (commit)` / `Écarté (raison)`.

### Lot 4 — Correctness critique

| Item | Audit | Description | Fichiers principaux | Statut | Notes relecture |
|---|---|---|---|---|---|
| 4.1 | A1 | `run_in_transaction` → `conn.transaction()` diesel-async (cancellation-safe) | `src/db/mod.rs` | Fait (c7cfe58) | Relu : délégation à `AsyncConnection::transaction` (manager → `is_broken` bb8), signature quasi inchangée (`+ Send`), zéro changement aux 8 call-sites, pas d'imbrication. 3 régressions `test_audit2_a1_*` vertes ; test d'annulation **rouge avec fix reverti** (observer voit `[]`). Helpers ajoutés : `setup_test_db_with_pool_size`, `TestApp.url`. |
| 4.2 | A2 | Ligne outbox `start` complétée dans la même tx que `mark_task_running` + gate start-before-end borné par fraîcheur | `src/workers/start_loop.rs`, `src/db/webhook_execution.rs`, `src/workers/delivery_loop.rs`, `src/main.rs` | Fait | Relu : `mark_task_running` + `complete_webhook_execution` en une tx (fenêtre de crash fermée sur le chemin nominal) ; gate borné par `start_stale_secs` (= `WORKER_CLAIM_TIMEOUT_SECS`, défaut 30 s) dans les 2 variantes de claim (la non-leased est morte, borne miroir quand même). Nuance actée : sur `Err` de la tx, la complétion rollback avec (avant : tentée séparément) — filet = borne de fraîcheur. 3 régressions `test_audit2_a2_*` ; contre-épreuve : gate débridé → les 2 tests de gate échouent. Suites outbox/delivery_lease/batch_complete + complète (193) vertes. CLAUDE.md mis à jour. |
| 4.3 | A4 | Fenêtre Claimed : sauver les cancel actions même si `mark_task_running` = false ; enqueuer le cancel outbox pour les Claimed (cancel_task + stop_batch) | `src/workers/start_loop.rs`, `src/workers/propagation.rs`, `src/db/task_lifecycle.rs` | À faire | |
| 4.4 | A7 | Flush compteurs : garde `status IN ('running','claimed')` + clamp `LEAST(...)` + anti-poison (bisect/per-row + drop loggé) + drain du canal avant flush final (C6 workers) | `src/workers/batch_updater.rs` | À faire | |
| 4.5 | A3 | Paused : interdire pause d'une Waiting (ou inclure Paused dans la propagation), décider pour Running, endpoint resume contextuel (`wait_* > 0 ? Waiting : Pending`), corriger la doc OpenAPI | `src/db/task_lifecycle.rs`, `src/workers/propagation.rs`, `src/handlers/task.rs`, `src/validation/task.rs` | À faire | Choix de design à trancher par l'utilisateur si ambigu |
| 4.6 | A8 | Dedupe inter-requêtes : `pg_advisory_xact_lock` sur la clé dedupe avant le COUNT | `src/db/task_crud.rs` | À faire | |
| 4.7 | A10 | Grab-bag : cancel d'une Waiting autorisé ; DELETE/pause → mapping ApiError (404/400/500) ; filtre `metadata` invalide → 400 ; PATCH idempotent (404/200/409 via SELECT de suivi) ; doc `metadata` merge vs replace | `src/handlers/task.rs`, `src/workers/propagation.rs`, `src/dtos/query.rs`, `src/db/task_lifecycle.rs` | À faire | Découpable en 2 si trop gros |
| 4.8 | A9 | (Optionnel) Ordre de lock des UPDATE `= ANY()` : pré-lock `ORDER BY id FOR UPDATE` ou retry 40P01 | `src/workers/propagation.rs`, `src/workers/batch_updater.rs` | À faire | Basse priorité |

### Lot 5 — Sécurité

| Item | Audit | Description | Fichiers principaux | Statut | Notes relecture |
|---|---|---|---|---|---|
| 5.1 | A5 | SSRF : `url.host()` (IPv6 parsé) + `to_ipv4_mapped()` dans `is_internal_ip` + resolver reqwest qui re-vérifie l'IP résolue à la livraison (anti-rebinding) | `src/validation/ssrf.rs`, `src/action.rs` | À faire | |
| 5.2 | A6 | Auth bearer token statique (env), `/health` `/ready` exclus ; Swagger + `/metrics` gated en release | `src/main.rs`, `src/handlers/mod.rs`, `src/config.rs` | À faire | HMAC du `?handle=` reporté au Lot 7 (breaking) |
| 5.3 | A10 | Limites structurelles POST /task : `MAX_TASKS_PER_BATCH`, `MAX_DEPS_PER_TASK`, `MAX_ACTIONS_PER_TASK`, `JsonConfig::limit` explicite, DFS itératif (ou suppression — l'ordre exclut déjà les cycles), chunk de `flush_run` sous les 65 535 bind params | `src/validation/`, `src/main.rs`, `src/db/task_crud.rs` | À faire | |

### Lot 6 — Perf non-breaking

| Item | Audit | Description | Fichiers principaux | Statut | Notes relecture |
|---|---|---|---|---|---|
| 6.1 | B1 | on_start : libérer la connexion DB pendant le HTTP (découpage en phases façon delivery loop) + valider `webhook_concurrency < pool_max` (pas seulement `>`) | `src/workers/start_loop.rs`, `src/config.rs` | À faire | |
| 6.2 | B3 | Index partiel `idx_task_batch_active ON task(batch_id) WHERE status NOT IN (terminaux)` + prédicat de non-vacuité dans le statement `FOR UPDATE` du batch | migration, `src/db/webhook_execution.rs` | À faire | Valider par EXPLAIN comme au Lot 0 |
| 6.3 | B5 | Cascade d'échec par niveau (frontière `eq_any` par niveau, O(profondeur)) | `src/workers/propagation.rs` | À faire | |
| 6.4 | B4 | Wake-ups `tokio::sync::Notify` : handlers → start loop, transitions → delivery loop ; polls conservés en fallback | `src/main.rs`, `src/workers/*` | À faire | |
| 6.5 | B2 | Heartbeat des claims en attente de permit (bump `last_updated`) puis monter les défauts de concurrency | `src/workers/start_loop.rs` | À faire | Après 6.1 |
| 6.6 | B6/B7 | Hygiène : POST /task → `BasicTaskDto` (conformité OpenAPI) ; tiebreaker `id` sur toutes les paginations ; drop index morts (`idx_action_task_id`, `idx_action_trigger`, `idx_task_kind` après pg_stat) ; `id` ajouté à `idx_task_priority` ; UUIDv7 ; `RETENTION_ENABLED` défaut on ou warning ; select ciblés (JSONB over-fetch) ; LIMIT + drain timeout_loop ; timeout court sur les probes health ; fixes circuit breaker (comptage par échec, probe unique HalfOpen) | divers | À faire | Découpable ; chaque drop d'index validé par pg_stat/EXPLAIN |

### Lot 7 — Breaking (sur décision explicite, hors boucle par défaut)

| Item | Audit | Description | Statut |
|---|---|---|---|
| 7.1 | D4 | API v1 : enveloppe `{batch_id, tasks, deduped}`, `Idempotency-Key` sur POST /task, verbes dédiés, dépréciation du bare-array, handle signé HMAC | À faire |
| 7.2 | D2 | `batch.remaining` (remplace FOR UPDATE + NOT EXISTS) | À faire |
| 7.3 | D1/D7 | `rule_slot` + bundle multi-réplica | À faire |
| 7.4 | D5 | API pull `GET /work` | À faire |
| 7.5 | D3/D6 | Split outbox/ledger, dispatcher séparé, archive/partitions | À faire |

## Journal

- 2026-07-08 : audit livré (`AUDIT_2_CLAUDE.md`), campagne initialisée, aucun item démarré.
- 2026-07-08 : 4.1 (A1) fait — implémentation Opus, relecture Fable (diff, régressions vertes, contre-épreuve rouge sur revert, suite intégration complète). Incident environnement : Docker zombie (backend pid survivant), remède kill -9 + relaunch confirmé.
- 2026-07-08 : 4.2 (A2) fait — même process. Gate start-before-end borné par fraîcheur + complétion start atomique avec le passage à Running.
