use std::any::TypeId;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// Identity of a provided capability: a type plus a diagnostic name.
#[derive(Clone, Copy, Eq)]
pub struct ServiceId {
    type_id: TypeId,
    name: &'static str,
}

impl ServiceId {
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl PartialEq for ServiceId {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id && self.name == other.name
    }
}

impl Hash for ServiceId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
        self.name.hash(state);
    }
}

impl fmt::Debug for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

/// Typed handle to a service. Keys are compile-time constants, never strings.
pub struct ServiceKey<T: ?Sized + 'static> {
    id: ServiceId,
    marker: PhantomData<fn(&T)>,
}

impl<T: ?Sized + 'static> ServiceKey<T> {
    pub const fn id(&self) -> ServiceId {
        self.id
    }
}

impl<T: ?Sized + 'static> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized + 'static> Copy for ServiceKey<T> {}

/// Builds the typed constant for a capability.
pub const fn service_key<T: ?Sized + 'static>(name: &'static str) -> ServiceKey<T> {
    ServiceKey {
        id: ServiceId {
            type_id: TypeId::of::<T>(),
            name,
        },
        marker: PhantomData,
    }
}
