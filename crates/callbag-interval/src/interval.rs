use async_nursery::{Nurse, NurseExt};
use futures_timer::Delay;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};

use callbag_core::{Message, Source};

/// A callbag listenable source that sends incremental numbers every x milliseconds.
///
/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/index.js#L1-L10>
pub fn interval(
    period: Duration,
    nursery: impl Nurse<()> + Send + Sync + 'static,
) -> Source<usize> {
    (move |message| {
        if let Message::Handshake(sink) = message {
            let i = AtomicUsize::new(0);
            let interval_cleared = Arc::new(AtomicBool::new(false));
            nursery
                .nurse({
                    let sink = Arc::clone(&sink);
                    let interval_cleared = Arc::clone(&interval_cleared);
                    let mut interval = Delay::new(period);
                    async move {
                        loop {
                            Pin::new(&mut interval).await;
                            if interval_cleared.load(AtomicOrdering::Acquire) {
                                break;
                            }
                            interval.reset(period);
                            let i = i.fetch_add(1, AtomicOrdering::AcqRel);
                            sink(Message::Data(i));
                        }
                    }
                })
                .unwrap();
            sink(Message::Handshake(Arc::new(
                (move |message| {
                    if let Message::Error(_) | Message::Terminate = message {
                        interval_cleared.store(true, AtomicOrdering::Release);
                    }
                })
                .into(),
            )));
        }
    })
    .into()
}
