use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{Message, Source};

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
/// use arc_swap::ArcSwap;
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, skip};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = skip(3)(interval(Duration::from_millis(1_000), nursery.clone()));
///
/// for_each({
///     let vec = Arc::clone(&vec);
///     move |x| {
///         println!("{}", x);
///         vec.rcu(move |vec| {
///             let mut vec = (**vec).clone();
///             vec.push(x);
///             vec
///         });
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(7_500), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(vec.load()[..], [3, 4, 5, 6]);
/// ```
///
/// On a pullable source:
///
/// ```
/// use arc_swap::ArcSwap;
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
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = skip(4)(from_iter(Range::new(10, 20)));
///
/// for_each({
///     let vec = Arc::clone(&vec);
///     move |x| {
///         println!("{}", x);
///         vec.rcu(move |vec| {
///             let mut vec = (**vec).clone();
///             vec.push(x);
///             vec
///         });
///     }
/// })(source);
///
/// assert_eq!(vec.load()[..], [14, 15, 16, 17, 18, 19, 20]);
/// ```
pub fn skip<T: 'static, S>(max: usize) -> Box<dyn Fn(S) -> Source<T>>
where
    S: Into<Arc<Source<T>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<T>> = source.into();
        (move |message| {
            if let Message::Handshake(sink) = message {
                let skipped = Arc::new(AtomicUsize::new(0));
                let talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));
                source(Message::Handshake(Arc::new(
                    (move |message| match message {
                        Message::Handshake(source) => {
                            talkback.store(Some(source));
                            sink(Message::Handshake(Arc::new(
                                {
                                    let talkback = Arc::clone(&talkback);
                                    move |message| match message {
                                        Message::Handshake(_) => {
                                            panic!("sink handshake has already occurred");
                                        }
                                        Message::Data(_) => {
                                            panic!("sink must not send data");
                                        }
                                        Message::Pull => {
                                            let talkback = talkback.load();
                                            let source = talkback.as_ref().unwrap();
                                            source(Message::Pull);
                                        }
                                        Message::Error(error) => {
                                            let talkback = talkback.load();
                                            let source = talkback.as_ref().unwrap();
                                            source(Message::Error(error));
                                        }
                                        Message::Terminate => {
                                            let talkback = talkback.load();
                                            let source = talkback.as_ref().unwrap();
                                            source(Message::Terminate);
                                        }
                                    }
                                }
                                .into(),
                            )));
                        }
                        Message::Data(data) => {
                            if skipped.load(AtomicOrdering::Acquire) < max {
                                skipped.fetch_add(1, AtomicOrdering::AcqRel);
                                {
                                    let talkback = talkback.load();
                                    let talkback = talkback.as_ref().unwrap();
                                    talkback(Message::Pull);
                                }
                            } else {
                                sink(Message::Data(data));
                            }
                        }
                        Message::Pull => {
                            panic!("source must not pull");
                        }
                        Message::Error(error) => {
                            sink(Message::Error(error));
                        }
                        Message::Terminate => {
                            sink(Message::Terminate);
                        }
                    })
                    .into(),
                )))
            }
        })
        .into()
    })
}
