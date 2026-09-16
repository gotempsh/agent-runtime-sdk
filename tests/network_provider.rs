//! External-crate coverage for the object-safe private-network extension API.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::Arc;

use temps_agent_runtime::network::{
    NetworkAccess, NetworkError, NetworkInstanceSpec, NetworkProvider, NetworkProviderCapabilities,
    NetworkProviderId, NetworkProviderRegistry, NetworkSandboxRequirements, NetworkSession,
    NetworkSessionState, NetworkSessionStatus,
};
use temps_agent_runtime::CommandSpec;

struct FixtureProvider {
    id: NetworkProviderId,
}

struct WireGuardFixture {
    private_key: String,
}

#[async_trait::async_trait]
impl NetworkProvider for FixtureProvider {
    fn id(&self) -> NetworkProviderId {
        self.id.clone()
    }

    fn capabilities(&self) -> NetworkProviderCapabilities {
        NetworkProviderCapabilities::new(true, true, false)
    }

    async fn start(
        &self,
        _spec: NetworkInstanceSpec,
    ) -> Result<Arc<dyn NetworkSession>, NetworkError> {
        Ok(Arc::new(FixtureSession {
            provider: self.id.clone(),
        }))
    }
}

struct FixtureSession {
    provider: NetworkProviderId,
}

#[async_trait::async_trait]
impl NetworkSession for FixtureSession {
    async fn status(&self) -> NetworkSessionStatus {
        NetworkSessionStatus::new(NetworkSessionState::Running, "fixture connected")
    }

    async fn authenticate(&self) -> Result<NetworkSessionStatus, NetworkError> {
        Ok(self.status().await)
    }

    async fn deauthenticate(&self) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn stop(&self) -> Result<NetworkSessionStatus, NetworkError> {
        Ok(NetworkSessionStatus::new(
            NetworkSessionState::Stopped,
            "fixture stopped",
        ))
    }

    async fn restart(&self) -> Result<NetworkSessionStatus, NetworkError> {
        Ok(self.status().await)
    }

    async fn access(&self) -> Result<Arc<dyn NetworkAccess>, NetworkError> {
        Ok(Arc::new(FixtureAccess {
            provider: self.provider.clone(),
        }))
    }
}

struct FixtureAccess {
    provider: NetworkProviderId,
}

impl NetworkAccess for FixtureAccess {
    fn environment(&self) -> BTreeMap<OsString, OsString> {
        BTreeMap::from([(
            OsString::from("FIXTURE_NETWORK_PROVIDER"),
            OsString::from(self.provider.as_str()),
        )])
    }

    fn guidance(&self) -> String {
        format!("Use the {} fixture network", self.provider)
    }

    fn sandbox_requirements(&self) -> NetworkSandboxRequirements {
        NetworkSandboxRequirements::default()
    }
}

#[tokio::test]
async fn downstream_provider_registers_starts_and_applies_access() {
    let id = NetworkProviderId::new("fixture-vpn").unwrap();
    let provider: Arc<dyn NetworkProvider> = Arc::new(FixtureProvider { id: id.clone() });
    let mut registry = NetworkProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    assert!(registry.register(provider).is_err());
    let descriptors = registry.providers();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].id, id);
    assert!(descriptors[0].capabilities.interactive_authentication);

    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let spec = NetworkInstanceSpec::new("client-a", &state_dir)
        .unwrap()
        .with_provider_configuration(WireGuardFixture {
            private_key: "fixture-private-key".into(),
        });
    assert_eq!(
        spec.provider_configuration::<WireGuardFixture>()
            .unwrap()
            .private_key,
        "fixture-private-key"
    );
    let spec_debug = format!("{spec:?}");
    assert!(spec_debug.contains("[REDACTED]"));
    assert!(!spec_debug.contains("fixture-private-key"));
    let managed = registry.start(&id, spec).await.unwrap();
    assert_eq!(managed.provider_id(), &id);
    assert_eq!(
        managed.session().status().await.state,
        NetworkSessionState::Running
    );
    let access = managed.session().access().await.unwrap();
    let command = access.apply(CommandSpec::new("agent"));
    assert_eq!(
        command
            .environment
            .get(&OsString::from("FIXTURE_NETWORK_PROVIDER")),
        Some(&OsString::from("fixture-vpn"))
    );

    let duplicate = NetworkInstanceSpec::new("client-b", &state_dir).unwrap();
    assert!(matches!(
        registry.start(&id, duplicate).await,
        Err(NetworkError::StateDirectoryInUse { .. })
    ));
    let mut second_registry = NetworkProviderRegistry::new();
    second_registry
        .register(Arc::new(FixtureProvider { id: id.clone() }))
        .unwrap();
    let cross_registry = NetworkInstanceSpec::new("client-b", &state_dir).unwrap();
    assert!(matches!(
        second_registry.start(&id, cross_registry).await,
        Err(NetworkError::StateDirectoryInUse { .. })
    ));
    drop(managed);
    let replacement = NetworkInstanceSpec::new("client-c", &state_dir).unwrap();
    assert!(matches!(
        registry.start(&id, replacement).await,
        Err(NetworkError::StateDirectoryInUse { .. })
    ));

    let reusable_dir = temp.path().join("reusable-state");
    let reusable = NetworkInstanceSpec::new("client-d", &reusable_dir).unwrap();
    let reusable = registry.start(&id, reusable).await.unwrap();
    reusable.shutdown().await.unwrap();
    let replacement = NetworkInstanceSpec::new("client-e", &reusable_dir).unwrap();
    registry
        .start(&id, replacement)
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let ambiguous = NetworkInstanceSpec::new(
        "client-f",
        temp.path().join("missing").join("..").join("state"),
    )
    .unwrap();
    assert!(matches!(
        registry.start(&id, ambiguous).await,
        Err(NetworkError::InvalidConfiguration {
            field: "state_dir",
            ..
        })
    ));

    let mut status =
        NetworkSessionStatus::new(NetworkSessionState::NeedsAuthentication, "open the browser");
    status.authentication_url = Some("https://login.example/secret-token".into());
    let status_debug = format!("{status:?}");
    assert!(status_debug.contains("authentication_pending: true"));
    assert!(!status_debug.contains("secret-token"));
}
