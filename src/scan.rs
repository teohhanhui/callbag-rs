use arc_swap::ArcSwap;
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

/// Callbag operator that combines consecutive values from the same source.
///
/// It's essentially like [`Iterator::scan`], delivering a new accumulated value for each value
/// from the callbag source.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-scan/blob/4ade1071e52f53a4b712d38f4e975f52ce8710c8/readme.js#L28-L40>
///
/// # Examples
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, scan};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let iter_source = from_iter([1, 2, 3, 4, 5]);
/// let scanned = scan(|prev, x| prev + x, 0)(iter_source);
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x}");
///         actual.push(x);
///     }
/// })(scanned);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     [1, 3, 6, 10, 15]
/// );
/// ```
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(reducer))
)]
pub fn scan<
    #[cfg(not(feature = "tracing"))] I: 'static,
    #[cfg(feature = "tracing")] I: fmt::Debug + 'static,
    #[cfg(not(feature = "tracing"))] O: 'static,
    #[cfg(feature = "tracing")] O: fmt::Debug + 'static,
    F: 'static,
    S,
>(
    reducer: F,
    seed: O,
) -> Box<dyn Fn(S) -> Source<O>>
where
    O: Clone + Send + Sync,
    F: Fn(O, I) -> O + Clone + Send + Sync,
    S: Into<Arc<Source<I>>>,
{
    #[cfg(feature = "tracing")]
    let scan_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "tracing")]
        let _scan_fn_entered = scan_fn_span.enter();
        let source: Arc<Source<I>> = source.into();
        {
            let reducer = reducer.clone();
            let seed = seed.clone();
            #[cfg(feature = "tracing")]
            let scan_fn_span = scan_fn_span.clone();
            move |message| {
                instrument!(follows_from: &scan_fn_span, "scan", scan_span);
                trace!("from sink: {message:?}");
                if let Message::Handshake(sink) = message {
                    let acc = ArcSwap::from_pointee(seed.clone());
                    call!(
                        source,
                        Message::Handshake(Arc::new(
                            {
                                let reducer = reducer.clone();
                                #[cfg(feature = "tracing")]
                                let scan_span = scan_span.clone();
                                move |message| {
                                    instrument!(parent: &scan_span, "source_talkback");
                                    trace!("from source: {message:?}");
                                    match message {
                                        Message::Handshake(source) => {
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::new(
                                                    {
                                                        #[cfg(feature = "tracing")]
                                                        let scan_span = scan_span.clone();
                                                        move |message| {
                                                            instrument!(
                                                                parent: &scan_span,
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
                                                                    call!(
                                                                        source,
                                                                        Message::Pull,
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Error(error) => {
                                                                    call!(
                                                                        source,
                                                                        Message::Error(error),
                                                                        "to source: {message:?}"
                                                                    );
                                                                },
                                                                Message::Terminate => {
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
                                            acc.store(Arc::new(reducer(
                                                (**acc.load()).clone(),
                                                data,
                                            )));
                                            call!(
                                                sink,
                                                Message::Data((**acc.load()).clone()),
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
