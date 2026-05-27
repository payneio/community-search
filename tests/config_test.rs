use community_search::config::Config;
use std::env;
use std::sync::Mutex;

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn clear_env() {
    let keys: Vec<String> = env::vars()
        .filter(|(k, _)| k.starts_with("COMMUNITY_SEARCH_"))
        .map(|(k, _)| k)
        .collect();
    for key in keys {
        unsafe { env::remove_var(&key) };
    }
}

#[test]
fn defaults_apply_when_nothing_set() {
    let _lock = ENV_MUTEX.lock().unwrap();
    clear_env();

    let config = Config::from_env_only().expect("config should load with defaults");

    assert_eq!(config.bind_addr, "127.0.0.1");
    assert_eq!(config.port, 8080u16);
    assert_eq!(config.data_dir, std::path::PathBuf::from("./data"));
    assert!(config.admin_token.is_none());
}

#[test]
fn env_vars_override_defaults() {
    let _lock = ENV_MUTEX.lock().unwrap();
    clear_env();
    unsafe {
        env::set_var("COMMUNITY_SEARCH_BIND_ADDR", "0.0.0.0");
        env::set_var("COMMUNITY_SEARCH_PORT", "9090");
        env::set_var("COMMUNITY_SEARCH_DATA_DIR", "/tmp/cs-test");
        env::set_var("COMMUNITY_SEARCH_ADMIN_TOKEN", "secret-token");
    }

    let config = Config::from_env_only().expect("config should load from env vars");

    assert_eq!(config.bind_addr, "0.0.0.0");
    assert_eq!(config.port, 9090u16);
    assert_eq!(config.data_dir, std::path::PathBuf::from("/tmp/cs-test"));
    assert_eq!(config.admin_token, Some("secret-token".to_string()));

    clear_env();
}

#[test]
fn port_must_be_valid_integer() {
    let _lock = ENV_MUTEX.lock().unwrap();
    clear_env();
    unsafe {
        env::set_var("COMMUNITY_SEARCH_PORT", "not-a-number");
    }

    let err = Config::from_env_only().expect_err("should fail with invalid port");
    let msg = format!("{err:?}");
    assert!(
        msg.to_lowercase().contains("port"),
        "error message should mention PORT, got: {msg}"
    );

    clear_env();
}
