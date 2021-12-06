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
pub fn skip<T: 'static>(max: usize) -> Box<dyn Fn(Source<T>) -> Source<T>> {
    Box::new(move |source| {
        (move |message| {
            if let Message::Handshake(sink) = message {
                let sink = Arc::new(sink);
                let skipped = Arc::new(AtomicUsize::new(0));
                let talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));
                source(Message::Handshake(
                    (move |message| match message {
                        Message::Handshake(source) => {
                            talkback.store(Some(Arc::new(source)));
                            sink(Message::Handshake(
                                ({
                                    let talkback = talkback.clone();
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
                                })
                                .into(),
                            ));
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
                ))
            }
        })
        .into()
    })
}
