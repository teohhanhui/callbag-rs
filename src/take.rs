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
pub fn take<T: 'static>(max: usize) -> Box<dyn Fn(Source<T>) -> Source<T>> {
    Box::new(move |source| {
        (move |message| {
            if let Message::Handshake(sink) = message {
                let sink = Arc::new(sink);
                let taken = Arc::new(AtomicUsize::new(0));
                let source_talkback: Arc<ArcSwapOption<Source<T>>> =
                    Arc::new(ArcSwapOption::from(None));
                let end = Arc::new(AtomicBool::new(false));
                let talkback = {
                    let taken = taken.clone();
                    let end = end.clone();
                    let source_talkback = source_talkback.clone();
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
                };
                source(Message::Handshake(
                    (move |message| match message {
                        Message::Handshake(source) => {
                            source_talkback.store(Some(Arc::new(source)));
                            sink(Message::Handshake(talkback.clone().into()));
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
                ))
            }
        })
        .into()
    })
}
