use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{Message, Source};

/// Callbag operator that limits the amount of data sent by a source.
///
/// Works on either pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/index.js#L1-L30>
///
/// # Examples
///
/// On a listenable source:
///
/// ```
/// use arc_swap::ArcSwap;
/// use async_nursery::Nursery;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = take(3)(interval(Duration::from_millis(1_000), nursery.clone()));
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
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(vec.load()[..], [0, 1, 2]);
/// ```
///
/// On a pullable source:
///
/// ```
/// use arc_swap::ArcSwap;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, take};
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
/// let source = take(4)(from_iter(Range::new(100, 999)));
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
/// assert_eq!(vec.load()[..], [100, 101, 102, 103]);
/// ```
pub fn take<T: 'static, S>(max: usize) -> Box<dyn Fn(S) -> Source<T>>
where
    S: Into<Arc<Source<T>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<T>> = source.into();
        (move |message| {
            if let Message::Handshake(sink) = message {
                let taken = Arc::new(AtomicUsize::new(0));
                let source_talkback: Arc<ArcSwapOption<Source<T>>> =
                    Arc::new(ArcSwapOption::from(None));
                let end = Arc::new(AtomicBool::new(false));
                let talkback: Arc<Source<T>> = Arc::new(
                    {
                        let taken = Arc::clone(&taken);
                        let end = Arc::clone(&end);
                        let source_talkback = Arc::clone(&source_talkback);
                        move |message| match message {
                            Message::Handshake(_) => {
                                panic!("sink handshake has already occurred");
                            }
                            Message::Data(_) => {
                                panic!("sink must not send data");
                            }
                            Message::Pull => {
                                if taken.load(AtomicOrdering::Acquire) < max {
                                    let source_talkback = source_talkback.load();
                                    let source_talkback = source_talkback.as_ref().unwrap();
                                    source_talkback(Message::Pull);
                                }
                            }
                            Message::Error(error) => {
                                end.store(true, AtomicOrdering::Release);
                                let source_talkback = source_talkback.load();
                                let source_talkback = source_talkback.as_ref().unwrap();
                                source_talkback(Message::Error(error));
                            }
                            Message::Terminate => {
                                end.store(true, AtomicOrdering::Release);
                                let source_talkback = source_talkback.load();
                                let source_talkback = source_talkback.as_ref().unwrap();
                                source_talkback(Message::Terminate);
                            }
                        }
                    }
                    .into(),
                );
                source(Message::Handshake(Arc::new(
                    (move |message| match message {
                        Message::Handshake(source) => {
                            source_talkback.store(Some(source));
                            sink(Message::Handshake(Arc::clone(&talkback)));
                        }
                        Message::Data(data) => {
                            if taken.load(AtomicOrdering::Acquire) < max {
                                let taken = taken.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                sink(Message::Data(data));
                                if taken == max && !end.load(AtomicOrdering::Acquire) {
                                    end.store(true, AtomicOrdering::Release);
                                    {
                                        let source_talkback = source_talkback.load();
                                        let source_talkback = source_talkback.as_ref().unwrap();
                                        source_talkback(Message::Terminate);
                                    }
                                    sink(Message::Terminate);
                                }
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
