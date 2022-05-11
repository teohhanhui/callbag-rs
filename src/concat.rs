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

/// Callbag factory that concatenates the data from multiple (2 or more) callbag sources.
///
/// It starts each source at a time: waits for the previous source to end before starting the next
/// source.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/readme.js#L29-L64>
///
/// # Examples
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{concat, for_each, from_iter};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = concat!(from_iter(["10", "20", "30"]), from_iter(["a", "b"]));
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
///     ["10", "20", "30", "a", "b"]
/// );
/// ```
#[macro_export]
macro_rules! concat {
    ($($s:expr),* $(,)?) => {
        $crate::concat(::std::vec![$($s),*].into_boxed_slice())
    };
}

/// Callbag factory that concatenates the data from multiple (2 or more) callbag sources.
///
/// It starts each source at a time: waits for the previous source to end before starting the next
/// source.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/readme.js#L29-L64>
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
#[doc(hidden)]
pub fn concat<
    #[cfg(not(feature = "tracing"))] T: 'static,
    #[cfg(feature = "tracing")] T: fmt::Debug + 'static,
    #[cfg(not(feature = "tracing"))] S: 'static,
    #[cfg(feature = "tracing")] S: fmt::Debug + 'static,
>(
    sources: Box<[S]>,
) -> Source<T>
where
    S: Into<Arc<Source<T>>> + Send + Sync,
{
    #[cfg(feature = "tracing")]
    let concat_fn_span = Span::current();
    let sources: Arc<Box<[Arc<Source<T>>]>> =
        Arc::new(Vec::from(sources).into_iter().map(|s| s.into()).collect());
    (move |message| {
        instrument!(follows_from: &concat_fn_span, "concat", concat_span);
        trace!("from sink: {message:?}");
        if let Message::Handshake(sink) = message {
            let n = sources.len();
            let i = Arc::new(AtomicUsize::new(0));
            let source_talkback: Arc<ArcSwapOption<Source<T>>> =
                Arc::new(ArcSwapOption::from(None));
            let got_pull = Arc::new(AtomicBool::new(false));
            let talkback: Arc<Source<T>> = Arc::new(
                {
                    #[cfg(feature = "tracing")]
                    let concat_span = concat_span.clone();
                    let source_talkback = Arc::clone(&source_talkback);
                    let got_pull = Arc::clone(&got_pull);
                    move |message| {
                        instrument!(parent: &concat_span, "sink_talkback");
                        trace!("from sink: {message:?}");
                        match message {
                            Message::Handshake(_) => {
                                panic!("sink handshake has already occurred");
                            },
                            Message::Data(_) => {
                                panic!("sink must not send data");
                            },
                            Message::Pull => {
                                got_pull.store(true, AtomicOrdering::Release);
                                let source_talkback = source_talkback.load();
                                let source_talkback =
                                    source_talkback.as_ref().expect("source talkback not set");
                                call!(source_talkback, Message::Pull, "to source: {message:?}");
                            },
                            Message::Error(ref error) => {
                                let source_talkback = source_talkback.load();
                                let source_talkback =
                                    source_talkback.as_ref().expect("source talkback not set");
                                call!(
                                    source_talkback,
                                    Message::Error(Arc::clone(error)),
                                    "to source: {message:?}"
                                );
                            },
                            Message::Terminate => {
                                let source_talkback = source_talkback.load();
                                let source_talkback =
                                    source_talkback.as_ref().expect("source talkback not set");
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
            ({
                let next_ref: Arc<ArcSwapOption<Box<dyn Fn() + Send + Sync>>> =
                    Arc::new(ArcSwapOption::from(None));
                let next: Arc<Box<dyn Fn() + Send + Sync>> = Arc::new(Box::new({
                    #[cfg(feature = "tracing")]
                    let concat_span = concat_span.clone();
                    let sources = Arc::clone(&sources);
                    let sink = Arc::clone(&sink);
                    let i = Arc::clone(&i);
                    let next_ref = Arc::clone(&next_ref);
                    move || {
                        if i.load(AtomicOrdering::Acquire) == n {
                            call!(sink, Message::Terminate, "to sink: {message:?}");
                            return;
                        }
                        call!(
                            sources[i.load(AtomicOrdering::Acquire)],
                            Message::Handshake(Arc::new(
                                {
                                    #[cfg(feature = "tracing")]
                                    let concat_span = concat_span.clone();
                                    let sink = Arc::clone(&sink);
                                    let i = Arc::clone(&i);
                                    let source_talkback = Arc::clone(&source_talkback);
                                    let got_pull = Arc::clone(&got_pull);
                                    let talkback = Arc::clone(&talkback);
                                    let next_ref = Arc::clone(&next_ref);
                                    move |message| {
                                        instrument!(parent: &concat_span, "source_talkback");
                                        trace!("from source: {message:?}");
                                        match message {
                                            Message::Handshake(source) => {
                                                source_talkback.store(Some(source));
                                                if i.load(AtomicOrdering::Acquire) == 0 {
                                                    call!(
                                                        sink,
                                                        Message::Handshake(Arc::clone(&talkback)),
                                                        "to sink: {message:?}"
                                                    );
                                                } else if got_pull.load(AtomicOrdering::Acquire) {
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
                                            Message::Data(data) => {
                                                call!(
                                                    sink,
                                                    Message::Data(data),
                                                    "to sink: {message:?}"
                                                );
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
                                                i.fetch_add(1, AtomicOrdering::AcqRel);
                                                let next_ref = next_ref.load();
                                                let next = next_ref.as_ref().unwrap();
                                                next();
                                            },
                                        }
                                    }
                                }
                                .into(),
                            )),
                            "to source: {message:?}"
                        );
                    }
                }));
                next_ref.store(Some(Arc::clone(&next)));
                next
            })();
        }
    })
    .into()
}
