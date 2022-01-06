use arc_swap::ArcSwapOption;
use never::Never;
use std::{
    iter,
    sync::{Arc, RwLock},
};

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
/// use arc_swap::ArcSwap;
/// use async_executors::{Timer, TimerExt};
/// use async_nursery::{NurseExt, Nursery};
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, share};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let vec_1 = Arc::new(ArcSwap::from_pointee(vec![]));
/// let vec_2 = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = Arc::new(share(interval(Duration::from_millis(1_000), nursery.clone())));
///
/// for_each({
///     let vec = Arc::clone(&vec_1);
///     move |x| {
///         println!("{}", x);
///         vec.rcu(move |vec| {
///             let mut vec = (**vec).clone();
///             vec.push(x);
///             vec
///         });
///     }
/// })(Arc::clone(&source));
///
/// nursery
///     .clone()
///     .nurse({
///         let nursery = nursery.clone();
///         let vec = Arc::clone(&vec_2);
///         const DURATION: Duration = Duration::from_millis(3_500);
///         async move {
///             nursery.sleep(DURATION).await;
///             for_each(move |x| {
///                 println!("{}", x);
///                 vec.rcu(move |vec| {
///                     let mut vec = (**vec).clone();
///                     vec.push(x);
///                     vec
///                 });
///             })(source);
///         }
///     })?;
///
/// let nursery_out = nursery.timeout(Duration::from_millis(6_500), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(vec_1.load()[..], [0, 1, 2, 3, 4, 5]);
/// assert_eq!(vec_2.load()[..], [3, 4, 5]);
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Share a pullable source to two pullers:
///
/// ```
/// use arc_swap::{ArcSwap, ArcSwapOption};
/// use std::sync::Arc;
///
/// use callbag::{from_iter, share, Message};
///
/// let vec_1 = Arc::new(ArcSwap::from_pointee(vec![]));
/// let vec_2 = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = share(from_iter([10, 20, 30, 40, 50]));
///
/// let talkback = Arc::new(ArcSwapOption::from(None));
/// source(Message::Handshake(Arc::new(
///     {
///         let vec = Arc::clone(&vec_1);
///         let talkback = Arc::clone(&talkback);
///         move |message| {
///             if let Message::Handshake(sink) = message {
///                 talkback.store(Some(sink));
///             } else if let Message::Data(data) = message {
///                 println!("a{}", data);
///                 vec.rcu(move |vec| {
///                     let mut vec = (**vec).clone();
///                     vec.push(format!("a{}", data));
///                     vec
///                 });
///             }
///         }
///     }
///     .into()
/// )));
///
/// source(Message::Handshake(Arc::new(
///     {
///         let vec = Arc::clone(&vec_2);
///         move |message| {
///             if let Message::Data(data) = message {
///                 println!("b{}", data);
///                 vec.rcu(move |vec| {
///                     let mut vec = (**vec).clone();
///                     vec.push(format!("b{}", data));
///                     vec
///                 });
///             }
///         }
///     }
///     .into()
/// )));
///
/// let talkback = talkback.load();
/// let talkback = talkback.as_ref().ok_or("no talkback")?;
/// talkback(Message::Pull);
/// talkback(Message::Pull);
///
/// assert_eq!(vec_1.load()[..], ["a10", "a20"]);
/// assert_eq!(vec_2.load()[..], ["b10", "b20"]);
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
    let sinks = Arc::new(RwLock::new(vec![]));
    let source_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));

    (move |message| {
        let sinks = Arc::clone(&sinks);
        let source_talkback = Arc::clone(&source_talkback);
        if let Message::Handshake(sink) = message {
            {
                let sinks = &mut *sinks.write().unwrap();
                sinks.push(Arc::clone(&sink));
            }

            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let sinks = Arc::clone(&sinks);
                    let source_talkback = Arc::clone(&source_talkback);
                    let sink = Arc::clone(&sink);
                    move |message| match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        }
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        }
                        Message::Pull => {
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(Message::Pull);
                        }
                        Message::Error(_) | Message::Terminate => {
                            {
                                let mut sinks = sinks.write().unwrap();
                                let i = sinks.iter().position({
                                    let sink = Arc::clone(&sink);
                                    move |s| Arc::ptr_eq(s, &sink)
                                });
                                if let Some(i) = i {
                                    sinks.splice(i..i + 1, iter::empty());
                                }
                            }
                            if sinks.read().unwrap().is_empty() {
                                let source_talkback = source_talkback.load();
                                let source_talkback = source_talkback.as_ref().unwrap();
                                source_talkback(Message::Terminate);
                            }
                        }
                    }
                }
                .into(),
            );

            if sinks.read().unwrap().len() == 1 {
                source(Message::Handshake(Arc::new(
                    {
                        move |message: Message<T, Never>| {
                            if let Message::Handshake(source) = message.clone() {
                                source_talkback.store(Some(source));
                                sink(Message::Handshake(Arc::clone(&talkback)));
                            } else {
                                let sinks = {
                                    let sinks = &*sinks.read().unwrap();
                                    sinks.clone()
                                };
                                for s in sinks {
                                    s(message.clone());
                                }
                            }
                            if let Message::Error(_) | Message::Terminate = message {
                                let sinks = &mut *sinks.write().unwrap();
                                sinks.clear();
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
