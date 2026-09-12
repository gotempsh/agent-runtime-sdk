//! Optional compatibility check against an installed Nono binary.

#![cfg(feature = "nono")]

use temps_agent_runtime::nono::{ManagedProfile, NetworkPolicy, NonoManager};

/// Run explicitly with `cargo test --test nono_live -- --ignored`.
#[tokio::test]
#[ignore = "requires an installed nono CLI"]
async fn generated_profile_passes_nono_strict_validation() {
    let Some(nono) = std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|directory| directory.join("nono"))
            .find(|path| path.is_file())
    }) else {
        eprintln!("nono is not installed; skipping live validation");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let manager = NonoManager::new(nono, directory.path());
    let mut profile = ManagedProfile::new("compatibility test", "default");
    profile.network = NetworkPolicy::AllowDomains {
        domains: vec!["api.anthropic.com".into(), "api.openai.com".into()],
    };

    let path = manager.save(profile).await.unwrap();
    assert!(path.is_file());
    manager.validate_path(&path).await.unwrap();
}
