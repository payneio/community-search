//! Federation: peer-to-peer communication, fan-out search, health tracking.

pub mod discovered;
pub mod fanout;
pub mod gossip;
pub mod health;
pub mod peer;
pub mod storage;

#[cfg(test)]
mod tests {
    #[test]
    fn federation_module_is_reachable() {
        // Smoke test: the module compiles and is wired into the crate root.
        // The inner async fn is never called; it only proves that:
        //   - super::peer is reachable from the federation module,
        //   - PeerClient is a valid dyn-safe trait,
        //   - health_check exists as an async method on PeerClient.
        #[allow(dead_code)]
        async fn _check(client: &dyn super::peer::PeerClient) {
            let _ = client.health_check("").await;
        }
    }
}
