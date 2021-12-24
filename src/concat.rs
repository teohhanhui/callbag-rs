use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{Message, Source};

/// Callbag factory that concatenates the data from multiple (2 or more) callbag sources.
///
/// It starts each source at a time: waits for the previous source to end before starting the next
/// source.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/readme.js#L29-L64>
#[macro_export]
macro_rules! concat {
    ($($s:expr),* $(,)?) => {
        $crate::concat(vec![$($s),*].into_boxed_slice())
    };
}

/// Callbag factory that concatenates the data from multiple (2 or more) callbag sources.
///
/// It starts each source at a time: waits for the previous source to end before starting the next
/// source.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/readme.js#L29-L64>
pub fn concat<T: 'static, S: 'static>(sources: Box<[S]>) -> Source<T>
where
    S: Into<Arc<Source<T>>> + Send + Sync,
{
    let sources: Arc<Box<[Arc<Source<T>>]>> =
        Arc::new(Vec::from(sources).into_iter().map(|s| s.into()).collect());
    (move |message| {
        if let Message::Handshake(sink) = message {
            let n = sources.len();
            let i = Arc::new(AtomicUsize::new(0));
            let source_talkback: Arc<ArcSwapOption<Source<T>>> =
                Arc::new(ArcSwapOption::from(None));
            let got_pull = Arc::new(AtomicBool::new(false));
            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let source_talkback = Arc::clone(&source_talkback);
                    let got_pull = Arc::clone(&got_pull);
                    move |message| match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        }
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        }
                        Message::Pull => {
                            got_pull.store(true, AtomicOrdering::Release);
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(Message::Pull);
                        }
                        Message::Error(ref error) => {
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(Message::Error(error.clone()));
                        }
                        Message::Terminate => {
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(Message::Terminate);
                        }
                    }
                }
                .into(),
            );
            ({
                let next_ref: Arc<ArcSwapOption<Box<dyn Fn() + Send + Sync>>> =
                    Arc::new(ArcSwapOption::from(None));
                let next: Arc<Box<dyn Fn() + Send + Sync>> = Arc::new(Box::new({
                    let sources = Arc::clone(&sources);
                    let sink = Arc::clone(&sink);
                    let i = Arc::clone(&i);
                    let next_ref = Arc::clone(&next_ref);
                    move || {
                        if i.load(AtomicOrdering::Acquire) == n {
                            sink(Message::Terminate);
                            return;
                        }
                        sources[i.load(AtomicOrdering::Acquire)](Message::Handshake(Arc::new(
                            {
                                let sink = Arc::clone(&sink);
                                let i = Arc::clone(&i);
                                let source_talkback = Arc::clone(&source_talkback);
                                let got_pull = Arc::clone(&got_pull);
                                let talkback = Arc::clone(&talkback);
                                let next_ref = Arc::clone(&next_ref);
                                move |message| match message {
                                    Message::Handshake(source) => {
                                        source_talkback.store(Some(source));
                                        if i.load(AtomicOrdering::Acquire) == 0 {
                                            sink(Message::Handshake(Arc::clone(&talkback)));
                                        } else if got_pull.load(AtomicOrdering::Acquire) {
                                            let source_talkback = source_talkback.load();
                                            let source_talkback = source_talkback.as_ref().unwrap();
                                            source_talkback(Message::Pull);
                                        }
                                    }
                                    Message::Data(data) => {
                                        sink(Message::Data(data));
                                    }
                                    Message::Pull => {
                                        panic!("source must not pull");
                                    }
                                    Message::Error(error) => {
                                        sink(Message::Error(error));
                                    }
                                    Message::Terminate => {
                                        i.fetch_add(1, AtomicOrdering::AcqRel);
                                        let next_ref = next_ref.load();
                                        let next = next_ref.as_ref().unwrap();
                                        next();
                                    }
                                }
                            }
                            .into(),
                        )));
                    }
                }));
                next_ref.store(Some(Arc::clone(&next)));
                next
            })();
        }
    })
    .into()
}
