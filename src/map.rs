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

/// Callbag operator that applies a transformation on data passing through it.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/readme.js#L24-L29>
///
/// # Examples
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, map};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = map(|x| (x as f64 * 0.1) as usize)(from_iter([10, 20, 30, 40]));
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
///     [1, 2, 3, 4]
/// );
/// ```
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip(f)))]
pub fn map<
    #[cfg(not(feature = "tracing"))] I: 'static,
    #[cfg(feature = "tracing")] I: fmt::Debug + 'static,
    #[cfg(not(feature = "tracing"))] O: 'static,
    #[cfg(feature = "tracing")] O: fmt::Debug + 'static,
    F: 'static,
    S,
>(
    f: F,
) -> Box<dyn Fn(S) -> Source<O>>
where
    F: Fn(I) -> O + Clone + Send + Sync,
    S: Into<Arc<Source<I>>>,
{
    #[cfg(feature = "tracing")]
    let map_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "tracing")]
        let _map_fn_entered = map_fn_span.enter();
        let source: Arc<Source<I>> = source.into();
        {
            let f = f.clone();
            #[cfg(feature = "tracing")]
            let map_fn_span = map_fn_span.clone();
            move |message| {
                instrument!(follows_from: &map_fn_span, "map", map_span);
                trace!("from sink: {message:?}");
                if let Message::Handshake(sink) = message {
                    call!(
                        source,
                        Message::Handshake(Arc::new(
                            {
                                let f = f.clone();
                                #[cfg(feature = "tracing")]
                                let map_span = map_span.clone();
                                move |message| {
                                    instrument!(parent: &map_span, "source_talkback");
                                    trace!("from source: {message:?}");
                                    match message {
                                        Message::Handshake(source) => {
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::new(
                                                    {
                                                        #[cfg(feature = "tracing")]
                                                        let map_span = map_span.clone();
                                                        move |message| {
                                                            instrument!(
                                                                parent: &map_span,
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
                                            call!(
                                                sink,
                                                Message::Data(f(data)),
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
