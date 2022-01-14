use async_executors::Timer;
use async_nursery::{Nurse, NurseExt};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};

use crate::{Message, Source};

/// A callbag listenable source that sends incremental numbers every x milliseconds.
///
/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/index.js#L1-L10>
///
/// # Examples
///
/// ```
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = interval(Duration::from_millis(1_000), nursery.clone());
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x}");
///         actual.push(x);
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(4_500), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     [0, 1, 2, 3]
/// );
/// ```
pub fn interval(
    period: Duration,
    nursery: impl Nurse<()> + Timer + Send + Sync + Clone + 'static,
) -> Source<usize> {
    (move |message| {
        if let Message::Handshake(sink) = message {
            let i = AtomicUsize::new(0);
            let interval_cleared = Arc::new(AtomicBool::new(false));
            if let Err(err) = nursery.nurse({
                let nursery = nursery.clone();
                let sink = Arc::clone(&sink);
                let interval_cleared = Arc::clone(&interval_cleared);
                async move {
                    loop {
                        nursery.sleep(period).await;
                        if interval_cleared.load(AtomicOrdering::Acquire) {
                            break;
                        }
                        let i = i.fetch_add(1, AtomicOrdering::AcqRel);
                        sink(Message::Data(i));
                    }
                }
            }) {
                sink(Message::Error(Arc::new(err)));
                return;
            }
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
