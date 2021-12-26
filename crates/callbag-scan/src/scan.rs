use arc_swap::ArcSwap;
use callbag_core::{Message, Source};
use std::sync::Arc;

/// Callbag operator that combines consecutive values from the same source.
///
/// It's essentially like [`Iterator::scan`], delivering a new accumulated value for each value from
/// the callbag source.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-scan/blob/4ade1071e52f53a4b712d38f4e975f52ce8710c8/readme.js#L28-L40>
pub fn scan<I: 'static, O: 'static, F: 'static, S>(
    reducer: F,
    seed: O,
) -> Box<dyn Fn(S) -> Source<O>>
where
    O: Send + Sync + Clone,
    F: Fn(O, I) -> O + Send + Sync + Clone,
    S: Into<Arc<Source<I>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<I>> = source.into();
        {
            let reducer = reducer.clone();
            let seed = seed.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    let acc = ArcSwap::from_pointee(seed.clone());
                    source(Message::Handshake(Arc::new(
                        {
                            let reducer = reducer.clone();
                            move |message| match message {
                                Message::Handshake(source) => {
                                    sink(Message::Handshake(Arc::new(
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
                                    )));
                                }
                                Message::Data(data) => {
                                    acc.store(Arc::new(reducer((**acc.load()).clone(), data)));
                                    sink(Message::Data((**acc.load()).clone()));
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
                        }
                        .into(),
                    )));
                }
            }
        }
        .into()
    })
}
