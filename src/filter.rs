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

/// Callbag operator that conditionally lets data pass through.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/readme.js#L23-L36>
///
/// # Examples
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{filter, for_each, from_iter};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = filter(|x| x % 2 == 1)(from_iter([1, 2, 3, 4, 5]));
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
///     [1, 3, 5]
/// );
/// ```
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(condition))
)]
pub fn filter<
    #[cfg(not(feature = "tracing"))] I: 'static,
    #[cfg(feature = "tracing")] I: fmt::Debug + 'static,
    F: 'static,
    S,
>(
    condition: F,
) -> Box<dyn Fn(S) -> Source<I>>
where
    F: Fn(&I) -> bool + Clone + Send + Sync,
    S: Into<Arc<Source<I>>>,
{
    #[cfg(feature = "tracing")]
    let filter_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "tracing")]
        let _filter_fn_entered = filter_fn_span.enter();
        let source: Arc<Source<I>> = source.into();
        {
            let condition = condition.clone();
            #[cfg(feature = "tracing")]
            let filter_fn_span = filter_fn_span.clone();
            move |message| {
                instrument!(follows_from: &filter_fn_span, "filter", filter_span);
                trace!("from sink: {message:?}");
                if let Message::Handshake(sink) = message {
                    let talkback = Arc::new(ArcSwapOption::from(None));
                    call!(
                        source,
                        Message::Handshake(Arc::new(
                            {
                                let condition = condition.clone();
                                #[cfg(feature = "tracing")]
                                let filter_span = filter_span.clone();
                                move |message| {
                                    instrument!(parent: &filter_span, "source_talkback");
                                    trace!("from source: {message:?}");
                                    let talkback = Arc::clone(&talkback);
                                    match message {
                                        Message::Handshake(source) => {
                                            talkback.store(Some(source));
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::new(
                                                    {
                                                        #[cfg(feature = "tracing")]
                                                        let filter_span = filter_span.clone();
                                                        move |message| {
                                                            instrument!(
                                                                parent: &filter_span,
                                                                "sink_talkback"
                                                            );
                                                            trace!("from sink: {message:?}");
                                                            match message {
                                                                Message::Handshake(_) => {
                                                                    panic!(
                                                            "sink handshake has already occurred"
                                                        );
                                                                },
                                                                Message::Data(_) => {
                                                                    panic!(
                                                                        "sink must not send data"
                                                                    );
                                                                },
                                                                Message::Pull => {
                                                                    let talkback = talkback.load();
                                                                    let source = talkback
                                                                        .as_ref()
                                                                        .expect(
                                                                        "source talkback not set",
                                                                    );
                                                                    call!(
                                                                        source,
                                                                        Message::Pull,
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Error(error) => {
                                                                    let talkback = talkback.load();
                                                                    let source = talkback
                                                                        .as_ref()
                                                                        .expect(
                                                                        "source talkback not set",
                                                                    );
                                                                    call!(
                                                                        source,
                                                                        Message::Error(error),
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Terminate => {
                                                                    let talkback = talkback.load();
                                                                    let source = talkback
                                                                        .as_ref()
                                                                        .expect(
                                                                        "source talkback not set",
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
                                            if condition(&data) {
                                                call!(
                                                    sink,
                                                    Message::Data(data),
                                                    "to sink: {message:?}"
                                                );
                                            } else {
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
