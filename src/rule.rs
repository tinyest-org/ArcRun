use crate::models::StatusKind;
use diesel::deserialize::{self, FromSql};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, IsNull, Output};
use diesel::{deserialize::FromSqlRow, serialize::ToSql, sql_types::Jsonb};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::hash::{Hash, Hasher};
use std::io::Write;
use utoipa::ToSchema;

/// A concurrency control strategy. Discriminated by the `type` JSON field.
///
/// Two types are supported:
/// - `Concurency`: at most N tasks matching the criteria can be Running simultaneously.
/// - `Capacity`: the total remaining work (sum of `expected_count - success - failures`)
///   across Running tasks matching the criteria must be below a threshold.
///   `matcher.status` must be `Running`, and tasks using Capacity must set `expected_count`.
///
/// Both can be combined on the same task (AND semantics: all rules must pass).
///
/// ## Concurency Example
/// ```json
/// {
///   "type": "Concurency",
///   "max_concurency": 1,
///   "matcher": {
///     "status": "Running",
///     "kind": "clustering",
///     "fields": ["projectId"]
///   }
/// }
/// ```
///
/// ## Capacity Example
/// ```json
/// {
///   "type": "Capacity",
///   "max_capacity": 500,
///   "matcher": {
///     "status": "Running",
///     "kind": "clustering",
///     "fields": ["projectId"]
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
#[serde(tag = "type")]
pub enum Strategy {
    /// Limits the number of concurrent Running tasks matching specific criteria.
    Concurency(ConcurencyRule),
    /// Limits the total remaining work across Running tasks matching specific criteria.
    /// The candidate is allowed if `sum(remaining) < max_capacity`.
    Capacity(CapacityRule),
}

/// Array of concurrency strategies attached to a task. Empty array means no concurrency constraints.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[diesel(sql_type = Jsonb)]
#[derive(Default)]
pub struct Rules(pub Vec<Strategy>);

/// Defines a concurrency limit: "at most `max_concurency` tasks matching `matcher` can run simultaneously."
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct ConcurencyRule {
    /// Maximum number of concurrent tasks allowed. Must be a positive integer.
    /// Example: 1 means "only one at a time" (mutual exclusion).
    pub max_concurency: i32,
    /// Criteria for which Running tasks count toward the limit.
    pub matcher: Matcher,
}

/// Defines a capacity limit: "the total remaining work across Running tasks matching `matcher`
/// must be below `max_capacity` for a new task to start."
///
/// `remaining(task) = GREATEST(COALESCE(expected_count, 0) - success - failures, 0)`
///
/// The candidate's own `expected_count` is NOT counted in the sum — only already-Running
/// (and Claimed) tasks are. The candidate MUST have `expected_count` set (non-null),
/// otherwise it is blocked. `matcher.status` must be `Running`.
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct CapacityRule {
    /// Max total remaining allowed for Running tasks matching the criteria.
    /// The candidate is allowed if `sum_running < max_capacity`.
    pub max_capacity: i32,
    /// Criteria for which Running tasks count toward the capacity sum.
    pub matcher: Matcher,
}

/// Criteria for matching tasks. Used in concurrency rules and deduplication.
///
/// Two tasks "match" if they have the same `status`, same `kind`, and identical values for all
/// keys listed in `fields` (looked up in the task's `metadata` JSON).
#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Hash, Eq, ToSchema)]
pub struct Matcher {
    /// The task status to match on. For concurrency rules, this is typically `Running`.
    /// When set to `Running`, Claimed tasks are also counted to avoid oversubscription.
    pub status: StatusKind,
    /// The task kind to match on. Must be non-empty.
    pub kind: String,
    /// Metadata field keys to compare. Two tasks match only if their metadata values are equal
    /// for ALL listed keys. Example: `["projectId", "environment"]` means two tasks match only
    /// if both their `projectId` and `environment` metadata values are identical.
    pub fields: Vec<String>,
}

impl Matcher {
    /// Extract the metadata field values specified by this matcher from the given metadata.
    /// Returns `Ok(json_object)` with only the matched fields, or `Err(field_name)` if a
    /// required field is missing from the metadata.
    ///
    /// If `fields` is empty, returns `Ok({})`.
    /// If `metadata` is null/not-an-object and fields are required, returns `Err`.
    pub fn extract_metadata_fields(
        &self,
        metadata: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let mut result = json!({});
        for field in &self.fields {
            match metadata.get(field) {
                Some(v) => {
                    result[field] = v.clone();
                }
                None => {
                    return Err(field.clone());
                }
            }
        }
        Ok(result)
    }
}

/// Compute a deterministic i64 advisory lock key from a matcher and task metadata.
/// The key is derived from the matcher's kind, status, and the task's metadata values
/// for the matcher's fields.
fn compute_lock_key(
    prefix: Option<&str>,
    matcher: &Matcher,
    task_metadata: &serde_json::Value,
) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Some(p) = prefix {
        p.hash(&mut hasher);
    }
    matcher.kind.hash(&mut hasher);
    matcher.status.hash(&mut hasher);
    for field in &matcher.fields {
        field.hash(&mut hasher);
        if let Some(val) = task_metadata.get(field) {
            val.to_string().hash(&mut hasher);
        }
    }
    hasher.finish() as i64
}

/// Compute a deterministic i64 advisory lock key from a concurrency rule and task metadata.
pub fn concurrency_lock_key(rule: &ConcurencyRule, task_metadata: &serde_json::Value) -> i64 {
    compute_lock_key(None, &rule.matcher, task_metadata)
}

/// Compute a deterministic i64 advisory lock key for a capacity rule and task metadata.
/// Uses a "capacity" prefix to avoid collisions with concurrency lock keys.
pub fn capacity_lock_key(rule: &CapacityRule, task_metadata: &serde_json::Value) -> i64 {
    compute_lock_key(Some("capacity"), &rule.matcher, task_metadata)
}

/// Compute a deterministic i64 advisory lock key for a dedupe matcher and task
/// metadata. Used by `handle_dedupe` to serialize the check-then-act dedupe window
/// between concurrent `POST /task` requests (Audit 2 A8).
///
/// Uses a distinct "dedupe" prefix so its keys don't collide with concurrency
/// (no prefix) or capacity ("capacity") lock keys. A hash collision between two
/// *different* dedupe keys is harmless: it only causes unnecessary serialization,
/// never a false dedupe (the `COUNT(*)` remains the source of truth).
pub fn dedupe_lock_key(matcher: &Matcher, task_metadata: &serde_json::Value) -> i64 {
    compute_lock_key(Some("dedupe"), matcher, task_metadata)
}

impl ToSql<Jsonb, Pg> for Rules {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        // <serde_json::Value as FromSql<Jsonb, Pg>>::to_sql(
        // we should be able to use jsonb implementation but the borrow checker
        // doesn't like it
        out.write_all(&[1])?; // JSONB version
        out.write_all(&serde_json::to_vec(&self)?)?;
        Ok(IsNull::No)
    }
}

impl FromSql<Jsonb, Pg> for Rules {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(bytes)?;
        Ok(serde_json::from_value(value)?)
    }
}
