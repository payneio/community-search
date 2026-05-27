use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: &str = "1.0";
pub const VERSION_HEADER: &str = "X-CommunitySearch-Version";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compatibility {
    Compatible,
    MinorMismatch,
    Incompatible,
}

/// Pad a short version string to a full three-component semver.
///
/// - `"1"` → `"1.0.0"`
/// - `"1.0"` → `"1.0.0"`
/// - `"1.0.0"` → `"1.0.0"` (unchanged)
fn normalize(v: &str) -> String {
    match v.matches('.').count() {
        0 => format!("{v}.0.0"),
        1 => format!("{v}.0"),
        _ => v.to_string(),
    }
}

/// Compare `their_version` against [`PROTOCOL_VERSION`].
///
/// Both version strings are normalised to three components before parsing so
/// that short forms such as `"1.0"` are treated identically to `"1.0.0"`.
///
/// | Condition                               | Result            |
/// |-----------------------------------------|-------------------|
/// | Major **and** minor match exactly       | `Compatible`      |
/// | Same major, different minor             | `MinorMismatch`   |
/// | Different major                         | `Incompatible`    |
/// | Malformed / unparseable version string  | `Incompatible`    |
pub fn check_compatibility(their_version: &str) -> Compatibility {
    let ours = match semver::Version::parse(&normalize(PROTOCOL_VERSION)) {
        Ok(v) => v,
        Err(_) => return Compatibility::Incompatible,
    };

    let theirs = match semver::Version::parse(&normalize(their_version)) {
        Ok(v) => v,
        Err(_) => return Compatibility::Incompatible,
    };

    if ours.major != theirs.major {
        Compatibility::Incompatible
    } else if ours.minor != theirs.minor {
        Compatibility::MinorMismatch
    } else {
        Compatibility::Compatible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_is_compatible() {
        assert_eq!(check_compatibility("1.0"), Compatibility::Compatible);
        assert_eq!(check_compatibility("1.0.0"), Compatibility::Compatible);
    }

    #[test]
    fn higher_minor_is_minor_mismatch() {
        assert_eq!(check_compatibility("1.1"), Compatibility::MinorMismatch);
        assert_eq!(check_compatibility("1.5.3"), Compatibility::MinorMismatch);
    }

    #[test]
    fn different_major_is_incompatible() {
        assert_eq!(check_compatibility("2.0"), Compatibility::Incompatible);
        assert_eq!(check_compatibility("0.9"), Compatibility::Incompatible);
    }

    #[test]
    fn malformed_version_is_incompatible() {
        assert_eq!(check_compatibility(""), Compatibility::Incompatible);
        assert_eq!(check_compatibility("v1"), Compatibility::Incompatible);
        assert_eq!(check_compatibility("abc"), Compatibility::Incompatible);
    }
}
