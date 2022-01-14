use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{Message, Source};

/// Callbag factory that merges data from multiple callbag sources.
///
/// Works well with listenable sources, and while it may work for some pullable sources, it is only
/// designed for listenable sources.
///
/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/readme.js#L29-L60>
///
/// # Examples
///
/// ```
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval, merge};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = merge!(
///     interval(Duration::from_millis(100), nursery.clone()),
///     interval(Duration::from_millis(350), nursery.clone()),
/// );
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{}", x);
///         actual.push(x);
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(650), nursery_out);
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
///     [0, 1, 2, 0, 3, 4, 5]
/// );
/// ```
#[macro_export]
macro_rules! merge {
    ($($s:expr),* $(,)?) => {
        $crate::merge(vec![$($s),*].into_boxed_slice())
    };
}

/// Callbag factory that merges data from multiple callbag sources.
///
/// Works well with listenable sources, and while it may work for some pullable sources, it is only
/// designed for listenable sources.
///
/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/readme.js#L29-L60>
#[doc(hidden)]
pub fn merge<T: 'static, S: 'static>(sources: Box<[S]>) -> Source<T>
where
    S: Into<Arc<Source<T>>> + Send + Sync,
{
    let sources: Box<[Arc<Source<T>>]> = Vec::from(sources).into_iter().map(|s| s.into()).collect();
    (move |message| {
        if let Message::Handshake(sink) = message {
            let n = sources.len();
            let source_talkbacks: Arc<Vec<ArcSwapOption<Source<T>>>> = Arc::new({
                let mut source_talkbacks = Vec::with_capacity(n);
                source_talkbacks.resize_with(n, || ArcSwapOption::from(None));
                source_talkbacks
            });
            let start_count = Arc::new(AtomicUsize::new(0));
            let end_count = Arc::new(AtomicUsize::new(0));
            let ended = Arc::new(AtomicBool::new(false));
            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let source_talkbacks = Arc::clone(&source_talkbacks);
                    let ended = Arc::clone(&ended);
                    move |message| {
                        if let Message::Error(_) | Message::Terminate = message {
                            ended.store(true, AtomicOrdering::Release);
                        }
                        for source_talkback in source_talkbacks.iter() {
                            if let Some(source_talkback) = &*source_talkback.load() {
                                match message {
                                    Message::Handshake(_) => {
                                        panic!("sink handshake has already occurred");
                                    },
                                    Message::Data(_) => {
                                        panic!("sink must not send data");
                                    },
                                    Message::Pull => {
                                        source_talkback(Message::Pull);
                                    },
                                    Message::Error(ref error) => {
                                        source_talkback(Message::Error(error.clone()));
                                    },
                                    Message::Terminate => {
                                        source_talkback(Message::Terminate);
                                    },
                                }
                            }
                        }
                    }
                }
                .into(),
            );
            for i in 0..n {
                if ended.load(AtomicOrdering::Acquire) {
                    return;
                }
                sources[i](Message::Handshake(Arc::new(
                    {
                        let sink = Arc::clone(&sink);
                        let source_talkbacks = Arc::clone(&source_talkbacks);
                        let start_count = Arc::clone(&start_count);
                        let end_count = Arc::clone(&end_count);
                        let ended = Arc::clone(&ended);
                        let talkback = Arc::clone(&talkback);
                        move |message| match message {
                            Message::Handshake(source) => {
                                source_talkbacks[i].store(Some(source));
                                let start_count =
                                    start_count.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                if start_count == 1 {
                                    sink(Message::Handshake(Arc::clone(&talkback)));
                                }
                            },
                            Message::Data(data) => {
                                sink(Message::Data(data));
                            },
                            Message::Pull => {
                                panic!("source must not pull");
                            },
                            Message::Error(error) => {
                                ended.store(true, AtomicOrdering::Release);
                                for j in 0..n {
                                    if j != i {
                                        if let Some(source_talkback) = &*source_talkbacks[j].load()
                                        {
                                            source_talkback(Message::Terminate);
                                        }
                                    }
                                }
                                sink(Message::Error(error));
                            },
                            Message::Terminate => {
                                source_talkbacks[i].store(None);
                                let end_count = end_count.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                if end_count == n {
                                    sink(Message::Terminate);
                                }
                            },
                        }
                    }
                    .into(),
                )));
            }
        }
    })
    .into()
}
