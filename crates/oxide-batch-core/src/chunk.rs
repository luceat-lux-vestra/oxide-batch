//! Chunk sizing, counting, and progress values.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

/// A nonzero item limit for one chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkSize(NonZeroU32);

impl ChunkSize {
    /// Constructs a nonzero chunk size.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ZeroSize`] when `value` is zero.
    pub fn new(value: u32) -> Result<Self, ChunkError> {
        NonZeroU32::new(value).map(Self).ok_or(ChunkError::ZeroSize)
    }

    /// Returns the configured item limit.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// A checked non-negative item or transaction count.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkCount(u64);

impl ChunkCount {
    /// The zero count.
    pub const ZERO: Self = Self(0);

    /// Constructs a count.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds two counts without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::CountOverflow`] when the sum exceeds `u64`.
    pub fn checked_add(self, other: Self) -> Result<Self, ChunkError> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(ChunkError::CountOverflow)
    }

    /// Increments this count without wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::CountOverflow`] at `u64::MAX`.
    pub fn checked_increment(self) -> Result<Self, ChunkError> {
        self.checked_add(Self(1))
    }
}

/// Validated item counts within one open chunk.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkCounts {
    read: ChunkCount,
    processed: ChunkCount,
    written: ChunkCount,
    filtered: ChunkCount,
}

impl ChunkCounts {
    /// Validates a complete count snapshot.
    ///
    /// `processed` counts items that produced writer input; `filtered` counts
    /// items intentionally producing no output. Their sum cannot exceed
    /// `read`, and `written` cannot exceed `processed`.
    ///
    /// # Errors
    ///
    /// Returns a typed overflow or invalid-state classification.
    pub fn new(
        read: ChunkCount,
        processed: ChunkCount,
        written: ChunkCount,
        filtered: ChunkCount,
    ) -> Result<Self, ChunkError> {
        let classified = processed.checked_add(filtered)?;
        if classified > read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        if written > processed {
            return Err(ChunkError::WrittenExceedsProcessed);
        }
        Ok(Self {
            read,
            processed,
            written,
            filtered,
        })
    }

    /// Returns the read count.
    #[must_use]
    pub const fn read(self) -> ChunkCount {
        self.read
    }

    /// Returns the successfully processed count.
    #[must_use]
    pub const fn processed(self) -> ChunkCount {
        self.processed
    }

    /// Returns the acknowledged writer-input count.
    #[must_use]
    pub const fn written(self) -> ChunkCount {
        self.written
    }

    /// Returns the filtered count.
    #[must_use]
    pub const fn filtered(self) -> ChunkCount {
        self.filtered
    }

    /// Adds two snapshots and revalidates their aggregate invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed overflow or invalid-state classification.
    pub fn checked_add(self, other: Self) -> Result<Self, ChunkError> {
        Self::new(
            self.read.checked_add(other.read)?,
            self.processed.checked_add(other.processed)?,
            self.written.checked_add(other.written)?,
            self.filtered.checked_add(other.filtered)?,
        )
    }
}

/// Mutable, invariant-preserving progress for one bounded chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkProgress {
    size: ChunkSize,
    counts: ChunkCounts,
}

impl ChunkProgress {
    /// Starts an empty chunk with a validated nonzero size.
    #[must_use]
    pub const fn new(size: ChunkSize) -> Self {
        Self {
            size,
            counts: ChunkCounts {
                read: ChunkCount::ZERO,
                processed: ChunkCount::ZERO,
                written: ChunkCount::ZERO,
                filtered: ChunkCount::ZERO,
            },
        }
    }

    /// Restores a validated in-memory progress snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::SizeExceeded`] when `counts.read()` exceeds the
    /// configured chunk size.
    pub fn from_counts(size: ChunkSize, counts: ChunkCounts) -> Result<Self, ChunkError> {
        if counts.read().get() > u64::from(size.get()) {
            return Err(ChunkError::SizeExceeded);
        }
        Ok(Self { size, counts })
    }

    /// Returns the configured size.
    #[must_use]
    pub const fn size(self) -> ChunkSize {
        self.size
    }

    /// Returns the current validated counts.
    #[must_use]
    pub const fn counts(self) -> ChunkCounts {
        self.counts
    }

    /// Returns whether no more items may be read into this chunk.
    #[must_use]
    pub fn is_full(self) -> bool {
        self.counts.read().get() == u64::from(self.size.get())
    }

    /// Records one successfully read item.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::SizeExceeded`] when the chunk is already full,
    /// or [`ChunkError::CountOverflow`] on arithmetic exhaustion.
    pub fn record_read(&mut self) -> Result<(), ChunkError> {
        if self.is_full() {
            return Err(ChunkError::SizeExceeded);
        }
        self.counts.read = self.counts.read.checked_increment()?;
        Ok(())
    }

    /// Classifies one previously read item as successfully processed.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ClassifiedExceedsRead`] when no unclassified read
    /// item remains, or [`ChunkError::CountOverflow`] on exhaustion.
    pub fn record_processed(&mut self) -> Result<(), ChunkError> {
        let next = self.counts.processed.checked_increment()?;
        let classified = next.checked_add(self.counts.filtered)?;
        if classified > self.counts.read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        self.counts.processed = next;
        Ok(())
    }

    /// Classifies one previously read item as filtered.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::ClassifiedExceedsRead`] when no unclassified read
    /// item remains, or [`ChunkError::CountOverflow`] on exhaustion.
    pub fn record_filtered(&mut self) -> Result<(), ChunkError> {
        let next = self.counts.filtered.checked_increment()?;
        let classified = self.counts.processed.checked_add(next)?;
        if classified > self.counts.read {
            return Err(ChunkError::ClassifiedExceedsRead);
        }
        self.counts.filtered = next;
        Ok(())
    }

    /// Records writer acknowledgement for `count` processed items.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::WrittenExceedsProcessed`] when the aggregate
    /// exceeds processed output, or [`ChunkError::CountOverflow`] on
    /// exhaustion.
    pub fn record_written(&mut self, count: ChunkCount) -> Result<(), ChunkError> {
        let next = self.counts.written.checked_add(count)?;
        if next > self.counts.processed {
            return Err(ChunkError::WrittenExceedsProcessed);
        }
        self.counts.written = next;
        Ok(())
    }
}

/// Stable chunk-size and count failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkError {
    /// Chunk size was zero.
    ZeroSize,
    /// Count arithmetic exceeded `u64`.
    CountOverflow,
    /// Processed plus filtered count exceeded read count.
    ClassifiedExceedsRead,
    /// Written count exceeded successfully processed count.
    WrittenExceedsProcessed,
    /// Read count exceeded the configured chunk size.
    SizeExceeded,
}

impl fmt::Display for ChunkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroSize => "chunk size must be nonzero",
            Self::CountOverflow => "chunk count arithmetic overflowed",
            Self::ClassifiedExceedsRead => "processed and filtered counts exceed the read count",
            Self::WrittenExceedsProcessed => "written count exceeds the processed count",
            Self::SizeExceeded => "read count exceeds the configured chunk size",
        })
    }
}

impl Error for ChunkError {}
