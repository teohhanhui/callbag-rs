use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "trace")]
use {std::fmt, tracing::Span};

/// Callbag sink that consumes both pullable and listenable sources.
///
/// When called on a pullable source, it will iterate through its data. When called on a listenable
/// source, it will observe its data.
///
/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/readme.js#L40-L47>
///
/// # Examples
///
/// Consume a pullable source:
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = from_iter([10, 20, 30, 40]);
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
///     [10, 20, 30, 40]
/// );
/// ```
///
/// Consume a listenable source:
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
#[cfg_attr(feature = "trace", tracing::instrument(level = "trace", skip(f)))]
pub fn for_each<
    #[cfg(not(feature = "trace"))] T: 'static,
    #[cfg(feature = "trace")] T: fmt::Debug + 'static,
    F: 'static,
    S,
>(
    f: F,
) -> Box<dyn Fn(S)>
where
    F: Fn(T) + Clone + Send + Sync,
    S: Into<Arc<Source<T>>>,
{
    #[cfg(feature = "trace")]
    let for_each_fn_span = Span::current();
    Box::new(move |source| {
        #[cfg(feature = "trace")]
        let _for_each_fn_entered = for_each_fn_span.enter();
        let source: Arc<Source<T>> = source.into();
        let talkback = ArcSwapOption::from(None);
        call!(
            source,
            Message::Handshake(Arc::new(
                {
                    let f = f.clone();
                    #[cfg(feature = "trace")]
                    let for_each_fn_span = for_each_fn_span.clone();
                    move |message| {
                        instrument!(follows_from: &for_each_fn_span, "for_each", for_each_span);
                        trace!("from source: {message:?}");
                        match message {
                            Message::Handshake(source) => {
                                talkback.store(Some(source));
                                let talkback = talkback.load();
                                let talkback = talkback.as_ref().expect("source talkback not set");
                                call!(talkback, Message::Pull, "to source: {message:?}");
                            },
                            Message::Data(data) => {
                                f(data);
                                let talkback = talkback.load();
                                let talkback = talkback.as_ref().expect("source talkback not set");
                                call!(talkback, Message::Pull, "to source: {message:?}");
                            },
                            Message::Pull => {
                                panic!("source must not pull");
                            },
                            Message::Error(_) => {},
                            Message::Terminate => {},
                        }
                    }
                }
                .into(),
            )),
            "to source: {message:?}"
        );
    })
}
