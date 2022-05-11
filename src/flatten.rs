use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "tracing")]
use {std::fmt, tracing::Span};

/// Callbag operator that flattens a higher-order callbag source.
///
/// Like [ReactiveX `switch`][reactivex-switch] or [xstream `flatten`][xstream-flatten]. Use it
/// with [`map`] to get behavior equivalent to [RxJS `switchMap`][rxjs-switch-map].
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/index.js#L1-L43>
///
/// # Examples
///
/// ## Listenables
///
/// ## Pullables
///
/// Loop over two iterables (such as arrays) and combine their values together:
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{flatten, for_each, from_iter, map, pipe};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = pipe!(
///     from_iter("hi".chars()),
///     map(|r#char| pipe!(
///         from_iter([10, 20, 30]),
///         map(move |num| format!("{char}{num}")),
///     )),
///     flatten,
///     for_each({
///         let actual = Arc::clone(&actual);
///         move |x: String| {
///             println!("{x}");
///             actual.push(x.clone());
///         }
///     }),
/// );
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     ["h10", "h20", "h30", "i10", "i20", "i30"]
/// );
/// ```
///
/// [`map`]: crate::map()
/// [reactivex-switch]: https://reactivex.io/documentation/operators/switch.html
/// [rxjs-switch-map]: https://rxjs.dev/api/operators/switchMap
/// [xstream-flatten]: https://github.com/staltz/xstream#flatten
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
pub fn flatten<
    #[cfg(not(feature = "tracing"))] T: 'static,
    #[cfg(feature = "tracing")] T: fmt::Debug + 'static,
    #[cfg(not(feature = "tracing"))] S,
    #[cfg(feature = "tracing")] S: fmt::Debug,
    #[cfg(not(feature = "tracing"))] R: 'static,
    #[cfg(feature = "tracing")] R: fmt::Debug + 'static,
>(
    source: S,
) -> Source<T>
where
    S: Into<Arc<Source<R>>>,
    R: Into<Arc<Source<T>>>,
{
    #[cfg(feature = "tracing")]
    let flatten_fn_span = Span::current();
    let source: Arc<Source<R>> = source.into();
    (move |message| {
        instrument!(follows_from: &flatten_fn_span, "flatten", flatten_span);
        trace!("from sink: {message:?}");
        if let Message::Handshake(sink) = message {
            let outer_talkback: Arc<ArcSwapOption<Source<R>>> = Arc::new(ArcSwapOption::from(None));
            let inner_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));
            let talkback: Arc<Source<T>> = Arc::new(
                {
                    #[cfg(feature = "tracing")]
                    let flatten_span = flatten_span.clone();
                    let outer_talkback = Arc::clone(&outer_talkback);
                    let inner_talkback = Arc::clone(&inner_talkback);
                    move |message| {
                        instrument!(parent: &flatten_span, "sink_talkback");
                        trace!("from sink: {message:?}");
                        match message {
                            Message::Handshake(_) => {
                                panic!("sink handshake has already occurred");
                            },
                            Message::Data(_) => {
                                panic!("sink must not send data");
                            },
                            Message::Pull => {
                                if let Some(inner_talkback) = &*inner_talkback.load() {
                                    call!(
                                        inner_talkback,
                                        Message::Pull,
                                        "to inner source: {message:?}"
                                    );
                                } else if let Some(outer_talkback) = &*outer_talkback.load() {
                                    call!(
                                        outer_talkback,
                                        Message::Pull,
                                        "to outer source: {message:?}"
                                    );
                                }
                            },
                            Message::Error(_) | Message::Terminate => {
                                if let Some(inner_talkback) = &*inner_talkback.load() {
                                    call!(
                                        inner_talkback,
                                        Message::Terminate,
                                        "to inner source: {message:?}"
                                    );
                                }
                                if let Some(outer_talkback) = &*outer_talkback.load() {
                                    call!(
                                        outer_talkback,
                                        Message::Terminate,
                                        "to outer source: {message:?}"
                                    );
                                }
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
                        let flatten_span = flatten_span.clone();
                        move |message| {
                            instrument!(parent: &flatten_span, "outer_source_talkback");
                            trace!("from outer source: {message:?}");
                            let sink = Arc::clone(&sink);
                            let outer_talkback = Arc::clone(&outer_talkback);
                            let inner_talkback = Arc::clone(&inner_talkback);
                            match message {
                                Message::Handshake(source) => {
                                    outer_talkback.store(Some(source));
                                    call!(
                                        sink,
                                        Message::Handshake(Arc::clone(&talkback)),
                                        "to sink: {message:?}"
                                    );
                                },
                                Message::Data(inner_source) => {
                                    let inner_source: Arc<Source<T>> = inner_source.into();
                                    if let Some(inner_talkback) = &*inner_talkback.load() {
                                        call!(
                                            inner_talkback,
                                            Message::Terminate,
                                            "to inner source: {message:?}"
                                        );
                                    }
                                    call!(
                                        inner_source,
                                        Message::Handshake(Arc::new(
                                            {
                                                #[cfg(feature = "tracing")]
                                                let flatten_span = flatten_span.clone();
                                                move |message| {
                                                    instrument!(
                                                        parent: &flatten_span,
                                                        "inner_source_talkback"
                                                    );
                                                    trace!("from inner source: {message:?}");
                                                    match message {
                                                        Message::Handshake(source) => {
                                                            inner_talkback.store(Some(source));
                                                            let inner_talkback =
                                                                inner_talkback.load();
                                                            let inner_talkback =
                                                                inner_talkback.as_ref().expect(
                                                                    "inner source talkback not set",
                                                                );
                                                            call!(
                                                                inner_talkback,
                                                                Message::Pull,
                                                                "to inner source: {message:?}"
                                                            );
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
                                                            if let Some(outer_talkback) =
                                                                &*outer_talkback.load()
                                                            {
                                                                call!(
                                                                    outer_talkback,
                                                                    Message::Terminate,
                                                                    "to outer source: {message:?}"
                                                                );
                                                            }
                                                            call!(
                                                                sink,
                                                                Message::Error(error),
                                                                "to sink: {message:?}"
                                                            );
                                                        },
                                                        Message::Terminate => {
                                                            if outer_talkback.load().is_none() {
                                                                call!(
                                                                    sink,
                                                                    Message::Terminate,
                                                                    "to sink: {message:?}"
                                                                );
                                                            } else {
                                                                inner_talkback.store(None);
                                                                let outer_talkback =
                                                                    outer_talkback.load();
                                                                let outer_talkback =
                                                            outer_talkback.as_ref().expect(
                                                                "outer source talkback not set",
                                                            );
                                                                call!(
                                                                    outer_talkback,
                                                                    Message::Pull,
                                                                    "to outer source: {message:?}"
                                                                );
                                                            }
                                                        },
                                                    }
                                                }
                                            }
                                            .into(),
                                        )),
                                        "to inner source: {message:?}"
                                    );
                                },
                                Message::Pull => {
                                    panic!("source must not pull");
                                },
                                Message::Error(error) => {
                                    if let Some(inner_talkback) = &*inner_talkback.load() {
                                        call!(
                                            inner_talkback,
                                            Message::Terminate,
                                            "to inner source: {message:?}"
                                        );
                                    }
                                    call!(sink, Message::Error(error), "to sink: {message:?}");
                                },
                                Message::Terminate => {
                                    if inner_talkback.load().is_none() {
                                        call!(sink, Message::Terminate, "to sink: {message:?}");
                                    } else {
                                        outer_talkback.store(None);
                                    }
                                },
                            }
                        }
                    }
                    .into(),
                )),
                "to outer source: {message:?}"
            );
        }
    })
    .into()
}
