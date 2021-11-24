use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{Message, Source};

/// Callbag operator that flattens a higher-order callbag source.
///
/// Like RxJS "switch" or xstream "flatten". Use it with map to get behavior equivalent to
/// "switchMap". Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/index.js#L1-L43>
pub fn flatten<T: 'static>(source: Source<Source<T>>) -> Source<T> {
    (move |message| {
        if let Message::Handshake(sink) = message {
            let sink = Arc::new(sink);
            let outer_talkback: Arc<ArcSwapOption<Source<Source<T>>>> =
                Arc::new(ArcSwapOption::from(None));
            let inner_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));
            let talkback = {
                let outer_talkback = outer_talkback.clone();
                let inner_talkback = inner_talkback.clone();
                move |message| match message {
                    Message::Handshake(_) => {
                        panic!("sink handshake has already occurred");
                    }
                    Message::Data(_) => {
                        panic!("sink must not send data");
                    }
                    Message::Pull => {
                        if let Some(inner_talkback) = &*inner_talkback.load() {
                            inner_talkback(Message::Pull);
                        } else if let Some(outer_talkback) = &*outer_talkback.load() {
                            outer_talkback(Message::Pull);
                        }
                    }
                    Message::Error(_) | Message::Terminate => {
                        if let Some(inner_talkback) = &*inner_talkback.load() {
                            inner_talkback(Message::Terminate);
                        }
                        if let Some(outer_talkback) = &*outer_talkback.load() {
                            outer_talkback(Message::Terminate);
                        }
                    }
                }
            };
            source(Message::Handshake(
                (move |message| {
                    let sink = sink.clone();
                    let outer_talkback = outer_talkback.clone();
                    let inner_talkback = inner_talkback.clone();
                    match message {
                        Message::Handshake(source) => {
                            outer_talkback.store(Some(Arc::new(source)));
                            sink(Message::Handshake(talkback.clone().into()));
                        }
                        Message::Data(inner_source) => {
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Terminate);
                            }
                            inner_source(Message::Handshake(
                                (move |message| match message {
                                    Message::Handshake(source) => {
                                        inner_talkback.store(Some(Arc::new(source)));
                                        let inner_talkback = inner_talkback.load();
                                        let inner_talkback = inner_talkback.as_ref().unwrap();
                                        inner_talkback(Message::Pull);
                                    }
                                    Message::Data(data) => {
                                        sink(Message::Data(data));
                                    }
                                    Message::Pull => {
                                        panic!("source must not pull");
                                    }
                                    Message::Error(error) => {
                                        if let Some(outer_talkback) = &*outer_talkback.load() {
                                            outer_talkback(Message::Terminate);
                                        }
                                        sink(Message::Error(error));
                                    }
                                    Message::Terminate => {
                                        if outer_talkback.load().is_none() {
                                            sink(Message::Terminate);
                                        } else {
                                            inner_talkback.store(None);
                                            let outer_talkback = outer_talkback.load();
                                            let outer_talkback = outer_talkback.as_ref().unwrap();
                                            outer_talkback(Message::Pull);
                                        }
                                    }
                                })
                                .into(),
                            ));
                        }
                        Message::Pull => {
                            panic!("source must not pull");
                        }
                        Message::Error(error) => {
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Terminate);
                            }
                            sink(Message::Error(error));
                        }
                        Message::Terminate => {
                            if inner_talkback.load().is_none() {
                                sink(Message::Terminate);
                            } else {
                                outer_talkback.store(None);
                            }
                        }
                    }
                })
                .into(),
            ));
        }
    })
    .into()
}
