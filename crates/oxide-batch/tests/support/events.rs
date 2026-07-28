use std::sync::{Arc, Mutex, PoisonError};

/// A stable event captured at a test boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedEvent {
    sequence: u64,
    name: String,
    detail: String,
}

impl CapturedEvent {
    /// Returns the zero-based capture sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the stable event name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns bounded test diagnostic detail.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// A cloneable, insertion-ordered test event sink.
#[derive(Clone, Debug, Default)]
pub struct EventCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl EventCapture {
    /// Creates an empty capture.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an event and returns its stable sequence number.
    pub fn record(&self, name: impl Into<String>, detail: impl Into<String>) -> u64 {
        let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
        let sequence = u64::try_from(events.len()).unwrap_or(u64::MAX);
        events.push(CapturedEvent {
            sequence,
            name: name.into(),
            detail: detail.into(),
        });
        sequence
    }

    /// Takes an insertion-ordered snapshot without clearing the capture.
    #[must_use]
    pub fn snapshot(&self) -> Vec<CapturedEvent> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Removes all captured events.
    pub fn clear(&self) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}
