use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_core::future::BoxFuture;

use crate::error::LifecycleError;
use crate::event::{Event, EventBus, Handler, Middleware, WaterfallEvent};
use crate::service::{ServiceId, ServiceKey};

type Disposer = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send + 'static>;

#[derive(Default)]
struct HostShared {
    services: Mutex<HashMap<ServiceId, Arc<dyn Any + Send + Sync>>>,
    bus: EventBus,
}

/// A registered initialization-time action. The host owns the disposal; the
/// handle is an opaque token, so callers never hand it back.
#[derive(Debug)]
pub struct Effect;

/// A replaceable, process-local unit with an explicit lifecycle.
pub trait Module: Send + Sync {
    fn name(&self) -> &'static str;

    /// Services that must be provided before this module initializes.
    fn dependencies(&self) -> &[ServiceId] {
        &[]
    }

    /// Services this module can use if present; absence is not an error.
    fn optional_dependencies(&self) -> &[ServiceId] {
        &[]
    }

    /// Services this module provides during `init`. Used for static ordering,
    /// missing-dependency detection and cycle reporting.
    fn provides(&self) -> &[ServiceId] {
        &[]
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>>;
}

/// Per-module view of the host. Cloneable so a module can keep it for runtime events.
#[derive(Clone)]
pub struct ModuleCtx {
    shared: Arc<HostShared>,
    effects: Arc<Mutex<Vec<Disposer>>>,
    module: &'static str,
}

impl ModuleCtx {
    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
    ) -> Result<Effect, LifecycleError> {
        let id = key.id();
        {
            let mut services = self.shared.services.lock().unwrap();
            if services.contains_key(&id) {
                return Err(LifecycleError::DuplicateProvider {
                    service: id.name().to_string(),
                });
            }
            services.insert(id, Arc::new(value));
        }

        let services = Arc::clone(&self.shared);
        self.effects.lock().unwrap().push(Box::new(move || {
            Box::pin(async move {
                services.services.lock().unwrap().remove(&id);
            })
        }));
        Ok(Effect)
    }

    pub fn require<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
    ) -> Result<Arc<T>, LifecycleError> {
        match self.lookup(key) {
            Some(value) => Ok(value),
            None => Err(LifecycleError::MissingDependency {
                module: self.module.to_string(),
                service: key.id().name().to_string(),
            }),
        }
    }

    pub fn require_optional<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
    ) -> Result<Option<Arc<T>>, LifecycleError> {
        Ok(self.lookup(key))
    }

    fn lookup<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Option<Arc<T>> {
        let service = self
            .shared
            .services
            .lock()
            .unwrap()
            .get(&key.id())
            .cloned()?;
        Some(
            service
                .downcast::<Arc<T>>()
                .expect("a ServiceId includes the service type")
                .as_ref()
                .clone(),
        )
    }

    pub fn emit<E: Event>(&self, event: E) -> BoxFuture<'_, ()> {
        self.shared.bus.emit(event)
    }

    pub fn waterfall<E: WaterfallEvent>(&self, event: E) -> BoxFuture<'_, Result<E, E::Error>> {
        self.shared.bus.waterfall(event)
    }

    pub fn on<E: Event>(&self, handler: impl Handler<E>) -> Effect {
        let registration = self.shared.bus.register_handler(Arc::new(handler));
        let bus = Arc::clone(&self.shared);
        self.effects.lock().unwrap().push(Box::new(move || {
            Box::pin(async move {
                bus.bus.remove_handler::<E>(registration);
            })
        }));
        Effect
    }

    pub fn intercept<E: WaterfallEvent>(&self, middleware: impl Middleware<E>) -> Effect {
        let registration = self.shared.bus.register_middleware(Arc::new(middleware));
        let bus = Arc::clone(&self.shared);
        self.effects.lock().unwrap().push(Box::new(move || {
            Box::pin(async move {
                bus.bus.remove_middleware::<E>(registration);
            })
        }));
        Effect
    }

    pub fn effect(
        &self,
        dispose: impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    ) -> Effect {
        self.effects.lock().unwrap().push(Box::new(dispose));
        Effect
    }
}

/// Explicit composition root: registers modules, initializes them in dependency
/// order and disposes their effects in reverse order.
#[derive(Default)]
pub struct ModuleHost {
    shared: Arc<HostShared>,
    modules: Vec<Arc<dyn Module>>,
    effects: Vec<Arc<Mutex<Vec<Disposer>>>>,
}

impl ModuleHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, module: Arc<dyn Module>) -> &mut Self {
        self.modules.push(module);
        self
    }

    /// Initialize every module in topological order.
    ///
    /// On failure the failing module's own effects are rolled back; effects of
    /// modules that initialized successfully stay registered until shutdown.
    pub async fn init(&mut self) -> Result<(), LifecycleError> {
        let order = self.topological_order()?;

        for index in order {
            let module = Arc::clone(&self.modules[index]);
            let effects: Arc<Mutex<Vec<Disposer>>> = Arc::new(Mutex::new(Vec::new()));
            let ctx = ModuleCtx {
                shared: Arc::clone(&self.shared),
                effects: Arc::clone(&effects),
                module: module.name(),
            };

            if let Err(source) = module.init(&ctx).await {
                let mut disposers = std::mem::take(&mut *effects.lock().unwrap());
                while let Some(dispose) = disposers.pop() {
                    dispose().await;
                }
                return Err(LifecycleError::InitFailed {
                    module: module.name().to_string(),
                    source: Box::new(source),
                });
            }

            self.effects.push(effects);
        }

        Ok(())
    }

    /// Dispose all effects in LIFO order, awaiting each disposer in turn.
    pub async fn shutdown(&mut self) {
        for effects in self.effects.iter().rev() {
            let mut disposers = std::mem::take(&mut *effects.lock().unwrap());
            while let Some(dispose) = disposers.pop() {
                dispose().await;
            }
        }
        self.effects.clear();
    }

    fn topological_order(&self) -> Result<Vec<usize>, LifecycleError> {
        let mut provider_of: HashMap<ServiceId, usize> = HashMap::new();
        for (index, module) in self.modules.iter().enumerate() {
            for id in module.provides() {
                if provider_of.insert(*id, index).is_some() {
                    return Err(LifecycleError::DuplicateProvider {
                        service: id.name().to_string(),
                    });
                }
            }
        }

        for module in &self.modules {
            for dependency in module.dependencies() {
                if !provider_of.contains_key(dependency) {
                    return Err(LifecycleError::MissingDependency {
                        module: module.name().to_string(),
                        service: dependency.name().to_string(),
                    });
                }
            }
        }

        let mut edges: Vec<Vec<usize>> = vec![Vec::new(); self.modules.len()];
        for (index, module) in self.modules.iter().enumerate() {
            for dependency in module
                .dependencies()
                .iter()
                .chain(module.optional_dependencies())
            {
                if let Some(&provider) = provider_of.get(dependency)
                    && provider != index
                {
                    // Edge points at the dependency; DFS post-order then
                    // initializes providers first.
                    edges[index].push(provider);
                }
            }
        }

        let mut state = vec![Visit::Unvisited; self.modules.len()];
        let mut stack = Vec::new();
        let mut order = Vec::new();
        for index in 0..self.modules.len() {
            if state[index] == Visit::Unvisited {
                visit(
                    index,
                    &self.modules,
                    &edges,
                    &mut state,
                    &mut stack,
                    &mut order,
                )?;
            }
        }
        Ok(order)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    Unvisited,
    Visiting,
    Done,
}

fn visit(
    index: usize,
    modules: &[Arc<dyn Module>],
    edges: &[Vec<usize>],
    state: &mut [Visit],
    stack: &mut Vec<usize>,
    order: &mut Vec<usize>,
) -> Result<(), LifecycleError> {
    state[index] = Visit::Visiting;
    stack.push(index);

    for &next in &edges[index] {
        match state[next] {
            Visit::Unvisited => visit(next, modules, edges, state, stack, order)?,
            Visit::Visiting => {
                let start = stack
                    .iter()
                    .position(|&node| node == next)
                    .expect("visiting node is on the stack");
                let mut path: Vec<String> = stack[start..]
                    .iter()
                    .map(|&node| modules[node].name().to_string())
                    .collect();
                path.push(modules[next].name().to_string());
                return Err(LifecycleError::Cycle { path });
            }
            Visit::Done => {}
        }
    }

    stack.pop();
    state[index] = Visit::Done;
    order.push(index);
    Ok(())
}
