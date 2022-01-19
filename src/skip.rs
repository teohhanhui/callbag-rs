use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "trace")]
use {std::fmt, tracing::Span};

/// Callbag operator that skips the first N data points of a source.
///
/// Works on either pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-skip/blob/698d6b7805c9bcddac038ceff25a0f0362adb25a/index.js#L1-L18>
///
/// # Examples
///
/// On a listenable source:
///
/// ```
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, skip};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = skip(3)(interval(Duration::from_millis(1_000), nursery.clone()));
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x}");
///         actual.push(x);
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(7_500), nursery_out);
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
///     [3, 4, 5, 6]
/// );
/// ```
///
/// On a pullable source:
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, skip};
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
/// let source = skip(4)(from_iter(Range::new(10, 20)));
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
///     [14, 15, 16, 17, 18, 19, 20]
/// );
/// ```
#[cfg_attr(feature = "trace", tracing::instrument(level = "trace"))]
pub fn skip<
    #[cfg(not(feature = "trace"))] T: 'static,
    #[cfg(feature = "trace")] T: fmt::Debug + 'static,
    S,
>(
    max: usize,
) -> Box<dyn Fn(S) -> Source<T>>
where
    S: Into<Arc<Source<T>>>,
{
    #[cfg(feature = "trace")]
    let skip_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "trace")]
        let _skip_fn_entered = skip_fn_span.enter();
        let source: Arc<Source<T>> = source.into();
        {
            #[cfg(feature = "trace")]
            let skip_fn_span = skip_fn_span.clone();
            move |message| {
                instrument!(follows_from: &skip_fn_span, "skip", skip_span);
                trace!("from sink: {message:?}");
                if let Message::Handshake(sink) = message {
                    let skipped = Arc::new(AtomicUsize::new(0));
                    let talkback: Arc<ArcSwapOption<Source<T>>> =
                        Arc::new(ArcSwapOption::from(None));
                    call!(
                        source,
                        Message::Handshake(Arc::new(
                            {
                                #[cfg(feature = "trace")]
                                let skip_span = skip_span.clone();
                                move |message| {
                                    instrument!(parent: &skip_span, "source_talkback");
                                    trace!("from source: {message:?}");
                                    match message {
                                        Message::Handshake(source) => {
                                            talkback.store(Some(source));
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::new(
                                                    {
                                                        #[cfg(feature = "trace")]
                                                        let skip_span = skip_span.clone();
                                                        let talkback = Arc::clone(&talkback);
                                                        move |message| {
                                                            instrument!(
                                                                parent: &skip_span,
                                                                "sink_talkback"
                                                            );
                                                            trace!("from sink: {message:?}");
                                                            match message {
                                                                Message::Handshake(_) => {
                                                                    panic!(
                                                                        "sink handshake has \
                                                                        already occurred"
                                                                    );
                                                                },
                                                                Message::Data(_) => {
                                                                    panic!(
                                                                        "sink must not send data"
                                                                    );
                                                                },
                                                                Message::Pull => {
                                                                    let talkback = talkback.load();
                                                                    let source =
                                                                        talkback.as_ref().expect(
                                                                            "source talkback not \
                                                                             set",
                                                                        );
                                                                    call!(
                                                                        source,
                                                                        Message::Pull,
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Error(error) => {
                                                                    let talkback = talkback.load();
                                                                    let source =
                                                                        talkback.as_ref().expect(
                                                                            "source talkback not \
                                                                             set",
                                                                        );
                                                                    call!(
                                                                        source,
                                                                        Message::Error(error),
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Terminate => {
                                                                    let talkback = talkback.load();
                                                                    let source =
                                                                        talkback.as_ref().expect(
                                                                            "source talkback not \
                                                                             set",
                                                                        );
                                                                    call!(
                                                                        source,
                                                                        Message::Terminate,
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                            }
                                                        }
                                                    }
                                                    .into(),
                                                )),
                                                "to sink: {message:?}"
                                            );
                                        },
                                        Message::Data(data) => {
                                            if skipped.load(AtomicOrdering::Acquire) < max {
                                                skipped.fetch_add(1, AtomicOrdering::AcqRel);
                                                {
                                                    let talkback = talkback.load();
                                                    let talkback = talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    call!(
                                                        talkback,
                                                        Message::Pull,
                                                        "to source: {message:?}"
                                                    );
                                                }
                                            } else {
                                                sink(Message::Data(data));
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
