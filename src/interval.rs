use async_executors::Timer;
use async_nursery::{Nurse, NurseExt};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "tracing")]
use {
    std::fmt,
    tracing::{trace_span, Span},
    tracing_futures::Instrument,
};

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
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
pub fn interval<
    #[cfg(not(feature = "tracing"))] N: 'static,
    #[cfg(feature = "tracing")] N: Instrument + fmt::Debug + 'static,
>(
    period: Duration,
    nursery: N,
) -> Source<usize>
where
    N: Nurse<()> + Timer + Clone + Send + Sync,
{
    #[cfg(feature = "tracing")]
    let interval_fn_span = Span::current();
    (move |message| {
        instrument!(follows_from: &interval_fn_span, "interval", interval_span);
        trace!("from sink: {message:?}");
        if let Message::Handshake(sink) = message {
            let i = AtomicUsize::new(0);
            let interval_cleared = Arc::new(AtomicBool::new(false));
            #[cfg(feature = "tracing")]
            let nursery = nursery
                .clone()
                .instrument(trace_span!(parent: &interval_span, "async_task"));
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
                        call!(sink, Message::Data(i), "to sink: {message:?}");
                    }
                }
            }) {
                call!(sink, Message::Error(Arc::new(err)), "to sink: {message:?}");
                return;
            }
            call!(
                sink,
                Message::Handshake(Arc::new(
                    {
                        #[cfg(feature = "tracing")]
                        let interval_span = interval_span.clone();
                        move |message| {
                            instrument!(parent: &interval_span, "sink_talkback");
                            trace!("from sink: {message:?}");
                            if let Message::Error(_) | Message::Terminate = message {
                                interval_cleared.store(true, AtomicOrdering::Release);
                            }
                        }
                    }
                    .into(),
                )),
                "to sink: {message:?}"
            );
        }
    })
    .into()
}
