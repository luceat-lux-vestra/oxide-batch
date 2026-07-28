/// A small, reproducible `SplitMix64` generator for test ordering and data.
///
/// The algorithm is intentionally fixed so a reported seed reproduces the
/// same sequence across platforms and dependency updates. It is not
/// cryptographically secure.
#[derive(Clone, Debug)]
pub struct SeededRandom {
    seed: u64,
    state: u64,
}

impl SeededRandom {
    /// Creates a generator from the seed included in failure diagnostics.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { seed, state: seed }
    }

    /// Returns the original reproduction seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Produces the next value in the stable `SplitMix64` sequence.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// Chooses an index without relying on registration or hash-map order.
    #[must_use]
    pub fn index(&mut self, length: usize) -> Option<usize> {
        if length == 0 {
            return None;
        }
        let length = u64::try_from(length).ok()?;
        usize::try_from(self.next_u64() % length).ok()
    }
}
