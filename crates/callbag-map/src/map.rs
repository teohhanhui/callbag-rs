use callbag_core::{Message, Source};
use std::sync::Arc;

/// Callbag operator that applies a transformation on data passing through it.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/readme.js#L24-L29>
pub fn map<I: 'static, O: 'static, F: 'static, S>(f: F) -> Box<dyn Fn(S) -> Source<O>>
where
    F: Fn(I) -> O + Send + Sync + Clone,
    S: Into<Arc<Source<I>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<I>> = source.into();
        {
            let f = f.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    source(Message::Handshake(Arc::new(
                        {
                            let f = f.clone();
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
                                    sink(Message::Data(f(data)));
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
