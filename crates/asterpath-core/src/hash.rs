//! Deterministic FNV-1a, shared by modules that derive stable ids.

/// Deterministic 128-bit FNV-1a; avoids a hashing dependency for stable ids.
pub struct Fnv(u64);

impl Fnv {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    pub fn finish_64(&self) -> u64 {
        self.0
    }

    /// Two differently seeded passes; the second consumes the first.
    pub fn finish_128(&self) -> [u8; 16] {
        let mut second = Fnv::new(self.0 ^ 0x9e3779b97f4a7c15);
        second.write(&self.0.to_be_bytes());
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.0.to_be_bytes());
        bytes[8..].copy_from_slice(&second.0.to_be_bytes());
        bytes
    }
}
