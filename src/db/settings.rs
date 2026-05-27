use anyhow::Result;
use rand::Rng;
use rusqlite::OptionalExtension;

use crate::db::Database;

const ADMIN_TOKEN_KEY: &str = "admin_token";

/// Return the value stored for `key`, or `None` if it is absent.
pub fn get(db: &Database, key: &str) -> Result<Option<String>> {
    let conn = db.conn();
    let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?1")?;
    let result = stmt
        .query_row(rusqlite::params![key], |row| row.get::<_, String>(0))
        .optional()?;
    Ok(result)
}

/// Upsert `key = value` in the settings table.
pub fn set(db: &Database, key: &str, value: &str) -> Result<()> {
    db.conn().execute(
        "INSERT INTO settings VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Ensure the admin token exists in the database.
///
/// - `explicit = Some(tok)`: write `tok` to DB (overwrite), return `(tok, false)`.
/// - `explicit = None`, DB already has a token: return `(existing, false)`.
/// - `explicit = None`, no token in DB: generate a fresh random token, persist
///   it, and return `(token, true)`. The caller is responsible for printing the
///   token to stdout exactly once (the `true` flag signals first-run).
pub fn ensure_admin_token(db: &Database, explicit: Option<&str>) -> Result<(String, bool)> {
    if let Some(tok) = explicit {
        set(db, ADMIN_TOKEN_KEY, tok)?;
        return Ok((tok.to_string(), false));
    }

    if let Some(existing) = get(db, ADMIN_TOKEN_KEY)? {
        return Ok((existing, false));
    }

    let token = generate_token();
    set(db, ADMIN_TOKEN_KEY, &token)?;
    Ok((token, true))
}

/// Generate a random 48-character token from an unambiguous alphabet.
///
/// Uses `ABCDEFGHJKMNPQRSTVWXYZ23456789` (no I, L, O, 0, or 1) so the token
/// is safe to read aloud and type without confusion.
fn generate_token() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTVWXYZ23456789";
    let mut rng = rand::thread_rng();
    (0..48)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}
