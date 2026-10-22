use std::sync::{Arc, Mutex};

use asterpath_core::{
    Effect, LifecycleError, Module, ModuleCtx, ModuleHost, ServiceId, ServiceKey, service_key,
};
use futures_util::future::BoxFuture;

trait Probe: Send + Sync {
    fn value(&self) -> u32;
}

struct ProbeImpl(u32);

impl Probe for ProbeImpl {
    fn value(&self) -> u32 {
        self.0
    }
}

const PROBE: ServiceKey<dyn Probe> = service_key::<dyn Probe>("Probe");
const OTHER: ServiceKey<dyn Probe> = service_key::<dyn Probe>("Other");

fn log() -> Arc<Mutex<Vec<&'static str>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn tag_effect(
    log: Arc<Mutex<Vec<&'static str>>>,
    tag: &'static str,
) -> impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static {
    move || {
        Box::pin(async move {
            log.lock().unwrap().push(tag);
        })
    }
}

struct Provider {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Module for Provider {
    fn name(&self) -> &'static str {
        "provider"
    }

    fn provides(&self) -> &[ServiceId] {
        static PROVIDES: [ServiceId; 1] = [PROBE.id()];
        &PROVIDES
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            self.log.lock().unwrap().push("provider");
            ctx.provide(PROBE, Arc::new(ProbeImpl(42)))?;
            Ok(())
        })
    }
}

struct Consumer {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Module for Consumer {
    fn name(&self) -> &'static str {
        "consumer"
    }

    fn dependencies(&self) -> &[ServiceId] {
        static DEPS: [ServiceId; 1] = [PROBE.id()];
        &DEPS
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            self.log.lock().unwrap().push("consumer");
            assert_eq!(ctx.require(PROBE)?.value(), 42);
            Ok(())
        })
    }
}

#[tokio::test]
async fn initializes_dependencies_before_dependants_regardless_of_registration_order() {
    let log = log();
    let mut host = ModuleHost::new();
    host.register(Arc::new(Consumer { log: log.clone() }));
    host.register(Arc::new(Provider { log: log.clone() }));

    host.init().await.unwrap();

    assert_eq!(*log.lock().unwrap(), ["provider", "consumer"]);
}

struct Node {
    name: &'static str,
    provides: ServiceId,
    deps: Vec<ServiceId>,
}

impl Module for Node {
    fn name(&self) -> &'static str {
        self.name
    }

    fn provides(&self) -> &[ServiceId] {
        std::slice::from_ref(&self.provides)
    }

    fn dependencies(&self) -> &[ServiceId] {
        &self.deps
    }

    fn init<'a>(&'a self, _ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn reports_cycle_with_its_path() {
    let mut host = ModuleHost::new();
    host.register(Arc::new(Node {
        name: "alpha",
        provides: PROBE.id(),
        deps: vec![OTHER.id()],
    }));
    host.register(Arc::new(Node {
        name: "beta",
        provides: OTHER.id(),
        deps: vec![PROBE.id()],
    }));

    let err = host.init().await.unwrap_err();
    let LifecycleError::Cycle { path } = err else {
        panic!("expected Cycle, got {err:?}");
    };
    assert!(path.len() >= 3, "cycle path must close the loop: {path:?}");
    assert_eq!(path.first(), path.last());
    assert!(path.contains(&"alpha".to_string()));
    assert!(path.contains(&"beta".to_string()));
}

struct Needy;

impl Module for Needy {
    fn name(&self) -> &'static str {
        "needy"
    }

    fn dependencies(&self) -> &[ServiceId] {
        static DEPS: [ServiceId; 1] = [PROBE.id()];
        &DEPS
    }

    fn init<'a>(&'a self, _ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn fails_startup_when_required_dependency_is_missing() {
    let mut host = ModuleHost::new();
    host.register(Arc::new(Needy));

    let err = host.init().await.unwrap_err();
    match err {
        LifecycleError::MissingDependency { module, service } => {
            assert_eq!(module, "needy");
            assert_eq!(service, "Probe");
        }
        other => panic!("expected MissingDependency, got {other:?}"),
    }
}

struct OptionalConsumer {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Module for OptionalConsumer {
    fn name(&self) -> &'static str {
        "optional"
    }

    fn optional_dependencies(&self) -> &[ServiceId] {
        static DEPS: [ServiceId; 1] = [PROBE.id()];
        &DEPS
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            assert!(ctx.require_optional(PROBE)?.is_none());
            self.log.lock().unwrap().push("optional");
            Ok(())
        })
    }
}

#[tokio::test]
async fn missing_optional_dependency_yields_none() {
    let log = log();
    let mut host = ModuleHost::new();
    host.register(Arc::new(OptionalConsumer { log: log.clone() }));

    host.init().await.unwrap();

    assert_eq!(*log.lock().unwrap(), ["optional"]);
}

struct DuplicateProvider {
    name: &'static str,
    value: u32,
}

impl Module for DuplicateProvider {
    fn name(&self) -> &'static str {
        self.name
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            ctx.provide(PROBE, Arc::new(ProbeImpl(self.value)))?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn duplicate_provide_is_a_startup_error() {
    let mut host = ModuleHost::new();
    host.register(Arc::new(DuplicateProvider {
        name: "first",
        value: 1,
    }));
    host.register(Arc::new(DuplicateProvider {
        name: "second",
        value: 2,
    }));

    let err = host.init().await.unwrap_err();
    match err {
        LifecycleError::InitFailed { module, source } => {
            assert_eq!(module, "second");
            assert!(
                matches!(*source, LifecycleError::DuplicateProvider { .. }),
                "expected DuplicateProvider, got {source:?}"
            );
        }
        other => panic!("expected InitFailed, got {other:?}"),
    }
}

struct Effectful {
    name: &'static str,
    effects: Vec<&'static str>,
    fail: bool,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Module for Effectful {
    fn name(&self) -> &'static str {
        self.name
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        Box::pin(async move {
            for tag in &self.effects {
                let _: Effect = ctx.effect(tag_effect(self.log.clone(), tag));
            }
            if self.fail {
                return Err(LifecycleError::MissingDependency {
                    module: self.name.to_string(),
                    service: "boom".to_string(),
                });
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn failed_init_rolls_back_only_its_own_effects_in_lifo_order() {
    let log = log();
    let mut host = ModuleHost::new();
    host.register(Arc::new(Effectful {
        name: "healthy",
        effects: vec!["a1", "a2"],
        fail: false,
        log: log.clone(),
    }));
    host.register(Arc::new(Effectful {
        name: "failing",
        effects: vec!["b1", "b2"],
        fail: true,
        log: log.clone(),
    }));

    assert!(host.init().await.is_err());

    assert_eq!(*log.lock().unwrap(), ["b2", "b1"]);
}

#[tokio::test]
async fn shutdown_disposes_effects_lifo_across_modules() {
    let log = log();
    let mut host = ModuleHost::new();
    host.register(Arc::new(Effectful {
        name: "first",
        effects: vec!["a1", "a2"],
        fail: false,
        log: log.clone(),
    }));
    host.register(Arc::new(Effectful {
        name: "second",
        effects: vec!["b1", "b2"],
        fail: false,
        log: log.clone(),
    }));

    host.init().await.unwrap();
    host.shutdown().await;

    assert_eq!(*log.lock().unwrap(), ["b2", "b1", "a2", "a1"]);
}
