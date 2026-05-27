use rand::distributions::Alphanumeric;
use rand::Rng;
use rusqlite::OptionalExtension;

use crate::db::Database;

const ADMIN_TOKEN_KEY: &str = "admin_token";
const TOKEN_LEN: usize = 48;

/// Resolve the admin token according to the following priority:
///
/// 1. If `env_override` is `Some(tok)`: persist `tok` (overwriting any stored
///    value) and return it.
/// 2. Otherwise, if a non-empty token is already stored in `app_config`,
///    return it unchanged.
/// 3. Otherwise, generate a fresh 48-character alphanumeric token, persist it
///    via UPSERT to `app_config`, and return it.
pub fn ensure_admin_token(
    db: &Database,
    env_override: Option<&str>,
) -> Result<String, rusqlite::Error> {
    if let Some(tok) = env_override {
        upsert_token(db, tok)?;
        return Ok(tok.to_owned());
    }

    if let Some(existing) = fetch_token(db)? {
        if !existing.is_empty() {
            return Ok(existing);
        }
    }

    let token: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(TOKEN_LEN)
        .map(char::from)
        .collect();

    upsert_token(db, &token)?;
    Ok(token)
}

/// Like [`ensure_admin_token`], but additionally prints the token to stdout
/// with a banner **only** when it is freshly generated (i.e. no pre-existing
/// token was stored and no `env_override` was supplied).
pub fn ensure_and_announce_admin_token(
    db: &Database,
    env_override: Option<&str>,
) -> Result<String, rusqlite::Error> {
    // Determine first-run condition *before* mutating state.
    let is_first_run =
        env_override.is_none() && fetch_token(db)?.map(|v| v.is_empty()).unwrap_or(true);

    let token = ensure_admin_token(db, env_override)?;

    if is_first_run {
        println!("================================================================");
        println!(" community-search: generated admin token (save this!):");
        println!("   {token}");
        println!(" Use as: Authorization: Bearer {token}");
        println!("================================================================");
    }

    Ok(token)
}

// ── private helpers ───────────────────────────────────────────────────────────

/// Read the current `admin_token` value from `app_config`.
///
/// Returns `None` when the row is absent or when its value is SQL `NULL`.
fn fetch_token(db: &Database) -> Result<Option<String>, rusqlite::Error> {
    let conn = db.conn();
    conn.query_row(
        "SELECT value FROM app_config WHERE key = ?1",
        rusqlite::params![ADMIN_TOKEN_KEY],
        |row| row.get::<_, Option<String>>(0),
    )
    .optional()
    .map(|opt| opt.flatten())
}

/// UPSERT `token` into `app_config` under the `admin_token` key.
fn upsert_token(db: &Database, token: &str) -> Result<(), rusqlite::Error> {
    db.conn().execute(
        "INSERT INTO app_config (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![ADMIN_TOKEN_KEY, token],
    )?;
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    /// A freshly generated token must be at least 32 chars, purely alphanumeric,
    /// and calling the function a second time must return the same persisted value.
    #[test]
    fn first_run_generates_and_persists_token_when_env_unset() {
        let db = Database::open_in_memory().expect("open in-memory db");

        let token1 = ensure_admin_token(&db, None).expect("first call");

        assert!(
            token1.len() >= 32,
            "token should be at least 32 chars, got: {}",
            token1.len()
        );
        assert!(
            token1.chars().all(|c| c.is_ascii_alphanumeric()),
            "token should be alphanumeric, got: {token1}"
        );

        let token2 = ensure_admin_token(&db, None).expect("second call");
        assert_eq!(
            token1, token2,
            "second call must return the same persisted token"
        );
    }

    /// Supplying an env override must persist it and return it; a subsequent
    /// call with `None` must return that persisted override value.
    #[test]
    fn env_override_replaces_any_stored_token() {
        let db = Database::open_in_memory().expect("open in-memory db");

        // Seed an initial generated token.
        let _initial = ensure_admin_token(&db, None).expect("generate initial token");

        // Apply an explicit override.
        let override_val = "override-token-for-test";
        let result = ensure_admin_token(&db, Some(override_val)).expect("apply override");
        assert_eq!(
            result, override_val,
            "should return the supplied override token"
        );

        // Subsequent no-override call must return the persisted override.
        let result2 = ensure_admin_token(&db, None).expect("retrieve after override");
        assert_eq!(
            result2, override_val,
            "should return the persisted override on next call with None"
        );
    }

    /// Two independent in-memory databases must produce distinct tokens.
    #[test]
    fn generated_tokens_are_unique() {
        let db1 = Database::open_in_memory().expect("open db1");
        let db2 = Database::open_in_memory().expect("open db2");

        let token1 = ensure_admin_token(&db1, None).expect("generate from db1");
        let token2 = ensure_admin_token(&db2, None).expect("generate from db2");

        assert_ne!(
            token1, token2,
            "independent DBs must produce different tokens"
        );
    }
}
