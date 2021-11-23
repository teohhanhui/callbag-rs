use std::sync::{Arc, RwLock};

use crate::{Message, Source};

/// Callbag operator that combines consecutive values from the same source.
///
/// It's essentially like array `.reduce`, but delivers a new accumulated value for each value from
/// the callbag source. Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-scan/blob/4ade1071e52f53a4b712d38f4e975f52ce8710c8/readme.js#L28-L40>
pub fn scan<I: 'static, O: 'static, F: 'static>(
    reducer: F,
    seed: O,
) -> Box<dyn Fn(Source<I>) -> Source<O>>
where
    O: Send + Sync + Clone,
    F: Fn(O, I) -> O + Send + Sync + Clone,
{
    Box::new(move |source| {
        ({
            let reducer = reducer.clone();
            let seed = seed.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    let acc = Arc::new(RwLock::new(seed.clone()));
                    source(Message::Handshake(
                        ({
                            let reducer = reducer.clone();
                            move |message| match message {
                                Message::Handshake(source) => {
                                    sink(Message::Handshake(
                                        (move |message| match message {
                                            Message::Handshake(_) => {
                                                panic!("sink handshake has already occurred");
                                            }
                                            Message::Data(_) => {
                                                panic!("sink must not send data");
                                            }
                                            Message::Pull => {
                                                source(Message::Pull);
                                            }
                                            Message::Error(error) => {
                                                source(Message::Error(error));
                                            }
                                            Message::Terminate => {
                                                source(Message::Terminate);
                                            }
                                        })
                                        .into(),
                                    ));
                                }
                                Message::Data(data) => {
                                    {
                                        let mut acc = acc.write().unwrap();
                                        *acc = reducer(acc.clone(), data);
                                    }
                                    let acc = {
                                        let acc = &*acc.read().unwrap();
                                        acc.clone()
                                    };
                                    sink(Message::Data(acc));
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
                            }
                        })
                        .into(),
                    ));
                }
            }
        })
        .into()
    })
}
