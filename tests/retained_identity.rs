//! Public API coverage: a downstream executor owns its shared identity registry.
use std::sync::Arc;
use temps_agent_runtime::lifecycle::{ConfigurationImpact, RuntimeId};
use temps_agent_runtime::retained::*;
use temps_agent_runtime::{EventSink, InteractionHandler, Provider, TurnRequest, TurnResult};

#[derive(Default)]
struct ExternalExecutor {
    identities: RetainedIdentityRegistry,
}
#[async_trait::async_trait]
impl RuntimeTurnExecutor for ExternalExecutor {
    fn claim_retained_identity(
        &self,
        id: &RuntimeId,
    ) -> RetainedRuntimeResult<Option<RetainedIdentityClaim>> {
        self.identities.claim(id).map(Some)
    }
    fn capabilities(&self, _: Provider) -> RuntimeDriverCapabilities {
        RuntimeDriverCapabilities::default()
    }
    fn configuration_impact(&self, _: RuntimeConfigurationKey) -> ConfigurationImpact {
        ConfigurationImpact::ReacquireRequired
    }
    async fn execute(
        &self,
        _: TurnRequest,
        _: &dyn EventSink,
        _: Option<&dyn InteractionHandler>,
    ) -> temps_agent_runtime::Result<TurnResult> {
        unreachable!("ownership test submits no provider turn")
    }
}

#[tokio::test]
async fn external_identity_claim_is_released_on_entry_drop_and_explicit_disposal() {
    let executor = Arc::new(ExternalExecutor::default());
    let first = InProcessRuntimeClient::from_executor(executor.clone());
    let second = InProcessRuntimeClient::from_executor(executor);
    let id = RuntimeId::new("external-owner").unwrap();
    let spec = RuntimeSpec::new(id.clone(), Provider::Claude, ".");
    let handle = first.acquire(spec.clone()).await.unwrap();
    assert!(second.acquire(spec.clone()).await.is_err());
    drop(first);
    assert!(second.acquire(spec.clone()).await.is_err());
    drop(handle);
    let old_handle = second.acquire(spec.clone()).await.unwrap();
    second.dispose(&id).await.unwrap();
    let replacement = second.acquire(spec.clone()).await.unwrap();
    drop(old_handle);
    assert!(
        second.acquire(spec).await.is_err(),
        "old guard must not release replacement"
    );
    drop(replacement);
    second.dispose(&id).await.unwrap();
}
