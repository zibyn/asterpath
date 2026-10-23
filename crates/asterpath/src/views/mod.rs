//! Read-side folds over the event log: trace spans and usage accounting.

pub(crate) mod trace;
pub(crate) mod usage;

#[cfg(test)]
mod tests;
