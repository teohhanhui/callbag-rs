use arc_swap::{ArcSwap, ArcSwapOption};
use never::Never;
use std::{iter, sync::Arc};

use crate::{Message, Source};

/// Callbag operator that broadcasts a single source to multiple sinks.
///
/// Does reference counting on sinks and starts the source when the first sink gets connected,
/// similar to [RxJS `.share()`][rxjs-share].
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/index.js#L1-L32>
///
/// # Examples
///
/// Share a listenable source to two listeners:
///
/// ```
/// use async_executors::{Timer, TimerExt};
/// use async_nursery::{NurseExt, Nursery};
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, share};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual_1 = Arc::new(SegQueue::new());
/// let actual_2 = Arc::new(SegQueue::new());
///
/// let source = Arc::new(share(interval(Duration::from_millis(1_000), nursery.clone())));
///
/// for_each({
///     let actual_1 = Arc::clone(&actual_1);
///     move |x| {
///         println!("{}", x);
///         actual_1.push(x);
///     }
/// })(Arc::clone(&source));
///
/// nursery
///     .nurse({
///         let nursery = nursery.clone();
///         let actual_2 = Arc::clone(&actual_2);
///         const DURATION: Duration = Duration::from_millis(3_500);
///         async move {
///             nursery.sleep(DURATION).await;
///             for_each(move |x| {
///                 println!("{}", x);
///                 actual_2.push(x);
///             })(source);
///         }
///     })?;
///
/// let nursery_out = nursery.timeout(Duration::from_millis(6_500), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual_1.len() {
///             v.push(actual_1.pop().unwrap());
///         }
///         v
///     }[..],
///     [0, 1, 2, 3, 4, 5]
/// );
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual_2.len() {
///             v.push(actual_2.pop().unwrap());
///         }
///         v
///     }[..],
///     [3, 4, 5]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Share a pullable source to two pullers:
///
/// ```
/// use arc_swap::ArcSwapOption;
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{from_iter, share, Message};
///
/// let actual_1 = Arc::new(SegQueue::new());
/// let actual_2 = Arc::new(SegQueue::new());
///
/// let source = share(from_iter([10, 20, 30, 40, 50]));
///
/// let talkback = Arc::new(ArcSwapOption::from(None));
/// source(Message::Handshake(Arc::new(
///     {
///         let actual_1 = Arc::clone(&actual_1);
///         let talkback = Arc::clone(&talkback);
///         move |message| {
///             if let Message::Handshake(source) = message {
///                 talkback.store(Some(source));
///             } else if let Message::Data(data) = message {
///                 println!("a{}", data);
///                 actual_1.push(format!("a{}", data));
///             }
///         }
///     }
///     .into()
/// )));
///
/// source(Message::Handshake(Arc::new(
///     {
///         let actual_2 = Arc::clone(&actual_2);
///         move |message| {
///             if let Message::Data(data) = message {
///                 println!("b{}", data);
///                 actual_2.push(format!("b{}", data));
///             }
///         }
///     }
///     .into()
/// )));
///
/// let talkback = talkback.load();
/// let talkback = talkback.as_ref().ok_or("source talkback not set")?;
/// talkback(Message::Pull);
/// talkback(Message::Pull);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual_1.len() {
///             v.push(actual_1.pop().unwrap());
///         }
///         v
///     }[..],
///     ["a10", "a20"]);
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual_2.len() {
///             v.push(actual_2.pop().unwrap());
///         }
///         v
///     }[..],
///     ["b10", "b20"]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [rxjs-share]: https://rxjs.dev/api/operators/share
pub fn share<T: 'static, S>(source: S) -> Source<T>
where
    T: Clone,
    S: Into<Arc<Source<T>>>,
{
    let source: Arc<Source<T>> = source.into();
    let sinks = Arc::new(ArcSwap::from_pointee(vec![]));
    let source_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));

    (move |message| {
        let sinks = Arc::clone(&sinks);
        let source_talkback = Arc::clone(&source_talkback);
        if let Message::Handshake(sink) = message {
            sinks.rcu({
                let sink = Arc::clone(&sink);
                move |sinks| {
                    let mut sinks = (**sinks).clone();
                    sinks.push(Arc::clone(&sink));
                    sinks
                }
            });

            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let sinks = Arc::clone(&sinks);
                    let source_talkback = Arc::clone(&source_talkback);
                    let sink = Arc::clone(&sink);
                    move |message| match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        },
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        },
                        Message::Pull => {
                            let source_talkback = source_talkback.load();
                            let source_talkback =
                                source_talkback.as_ref().expect("source talkback not set");
                            source_talkback(Message::Pull);
                        },
                        Message::Error(_) | Message::Terminate => {
                            {
                                let i = sinks.load().iter().position({
                                    let sink = Arc::clone(&sink);
                                    move |s| Arc::ptr_eq(s, &sink)
                                });
                                if let Some(i) = i {
                                    sinks.rcu(move |sinks| {
                                        let mut sinks = (**sinks).clone();
                                        sinks.splice(i..i + 1, iter::empty());
                                        sinks
                                    });
                                }
                            }
                            if sinks.load().is_empty() {
                                let source_talkback = source_talkback.load();
                                let source_talkback =
                                    source_talkback.as_ref().expect("source talkback not set");
                                source_talkback(Message::Terminate);
                            }
                        },
                    }
                }
                .into(),
            );

            if sinks.load().len() == 1 {
                source(Message::Handshake(Arc::new(
                    {
                        move |message: Message<T, Never>| {
                            if let Message::Handshake(source) = message.clone() {
                                source_talkback.store(Some(source));
                                sink(Message::Handshake(Arc::clone(&talkback)));
                            } else {
                                for s in &**sinks.load() {
                                    s(message.clone());
                                }
                            }
                            if let Message::Error(_) | Message::Terminate = message {
                                sinks.store(Arc::new(vec![]));
                            }
                        }
                    }
                    .into(),
                )));
                return;
            }

            sink(Message::Handshake(talkback));
        }
    })
    .into()
}
