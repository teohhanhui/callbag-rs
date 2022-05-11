use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "tracing")]
use {std::fmt, tracing::Span};

/// Callbag operator that limits the amount of data sent by a source.
///
/// Works on either pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/index.js#L1-L30>
///
/// # Examples
///
/// On a listenable source:
///
/// ```
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = take(3)(interval(Duration::from_millis(1_000), nursery.clone()));
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x}");
///         actual.push(x);
///     }
/// })(source);
///
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
///     [0, 1, 2]
/// );
/// ```
///
/// On a pullable source:
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, take};
///
/// #[derive(Clone)]
/// struct Range {
///     i: usize,
///     to: usize,
/// }
///
/// impl Range {
///     fn new(from: usize, to: usize) -> Self {
///         Range { i: from, to }
///     }
/// }
///
/// impl Iterator for Range {
///     type Item = usize;
///
///     fn next(&mut self) -> Option<Self::Item> {
///         let i = self.i;
///         if i <= self.to {
///             self.i += 1;
///             Some(i)
///         } else {
///             None
///         }
///     }
/// }
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = take(4)(from_iter(Range::new(100, 999)));
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x}");
///         actual.push(x);
///     }
/// })(source);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     [100, 101, 102, 103]
/// );
/// ```
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
pub fn take<
    #[cfg(not(feature = "tracing"))] T: 'static,
    #[cfg(feature = "tracing")] T: fmt::Debug + 'static,
    S,
>(
    max: usize,
) -> Box<dyn Fn(S) -> Source<T>>
where
    S: Into<Arc<Source<T>>>,
{
    #[cfg(feature = "tracing")]
    let take_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "tracing")]
        let _take_fn_entered = take_fn_span.enter();
        let source: Arc<Source<T>> = source.into();
        {
            #[cfg(feature = "tracing")]
            let take_fn_span = take_fn_span.clone();
            move |message| {
                instrument!(follows_from: &take_fn_span, "take", take_span);
                trace!("from sink: {message:?}");
                if let Message::Handshake(sink) = message {
                    let taken = Arc::new(AtomicUsize::new(0));
                    let source_talkback: Arc<ArcSwapOption<Source<T>>> =
                        Arc::new(ArcSwapOption::from(None));
                    let end = Arc::new(AtomicBool::new(false));
                    let talkback: Arc<Source<T>> = Arc::new(
                        {
                            #[cfg(feature = "tracing")]
                            let take_span = take_span.clone();
                            let taken = Arc::clone(&taken);
                            let end = Arc::clone(&end);
                            let source_talkback = Arc::clone(&source_talkback);
                            move |message| {
                                instrument!(parent: &take_span, "sink_talkback");
                                trace!("from sink: {message:?}");
                                match message {
                                    Message::Handshake(_) => {
                                        panic!("sink handshake has already occurred");
                                    },
                                    Message::Data(_) => {
                                        panic!("sink must not send data");
                                    },
                                    Message::Pull => {
                                        if taken.load(AtomicOrdering::Acquire) < max {
                                            let source_talkback = source_talkback.load();
                                            let source_talkback = source_talkback
                                                .as_ref()
                                                .expect("source talkback not set");
                                            call!(
                                                source_talkback,
                                                Message::Pull,
                                                "to source: {message:?}"
                                            );
                                        }
                                    },
                                    Message::Error(error) => {
                                        end.store(true, AtomicOrdering::Release);
                                        let source_talkback = source_talkback.load();
                                        let source_talkback = source_talkback
                                            .as_ref()
                                            .expect("source talkback not set");
                                        call!(
                                            source_talkback,
                                            Message::Error(error),
                                            "to source: {message:?}"
                                        );
                                    },
                                    Message::Terminate => {
                                        end.store(true, AtomicOrdering::Release);
                                        let source_talkback = source_talkback.load();
                                        let source_talkback = source_talkback
                                            .as_ref()
                                            .expect("source talkback not set");
                                        call!(
                                            source_talkback,
                                            Message::Terminate,
                                            "to source: {message:?}"
                                        );
                                    },
                                }
                            }
                        }
                        .into(),
                    );
                    call!(
                        source,
                        Message::Handshake(Arc::new(
                            {
                                #[cfg(feature = "tracing")]
                                let take_span = take_span.clone();
                                move |message| {
                                    instrument!(parent: &take_span, "source_talkback");
                                    trace!("from source: {message:?}");
                                    match message {
                                        Message::Handshake(source) => {
                                            source_talkback.store(Some(source));
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::clone(&talkback)),
                                                "to sink: {message:?}"
                                            );
                                        },
                                        Message::Data(data) => {
                                            if taken.load(AtomicOrdering::Acquire) < max {
                                                let taken =
                                                    taken.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                                call!(
                                                    sink,
                                                    Message::Data(data),
                                                    "to sink: {message:?}"
                                                );
                                                if taken == max
                                                    && !end.load(AtomicOrdering::Acquire)
                                                {
                                                    end.store(true, AtomicOrdering::Release);
                                                    {
                                                        let source_talkback =
                                                            source_talkback.load();
                                                        let source_talkback = source_talkback
                                                            .as_ref()
                                                            .expect("source talkback not set");
                                                        call!(
                                                            source_talkback,
                                                            Message::Terminate,
                                                            "to source: {message:?}"
                                                        );
                                                    }
                                                    call!(
                                                        sink,
                                                        Message::Terminate,
                                                        "to sink: {message:?}"
                                                    );
                                                }
                                            }
                                        },
                                        Message::Pull => {
                                            panic!("source must not pull");
                                        },
                                        Message::Error(error) => {
                                            call!(
                                                sink,
                                                Message::Error(error),
                                                "to sink: {message:?}"
                                            );
                                        },
                                        Message::Terminate => {
                                            call!(sink, Message::Terminate, "to sink: {message:?}");
                                        },
                                    }
                                }
                            }
                            .into(),
                        )),
                        "to source: {message:?}"
                    );
                }
            }
        }
        .into()
    })
}
