/// Ordered list of SQL migration scripts applied by `run_migrations()`.
///
/// 001_init.sql is the consolidated schema (folded from the original
/// chain of 15 migrations). New schema changes should be appended as
/// 002+, never folded back into 001.
pub(crate) const MIGRATIONS: &[&str] = &[include_str!("migrations/001_init.sql")];
