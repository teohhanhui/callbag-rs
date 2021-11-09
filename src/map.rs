use crate::{Message, Source};

/// Callbag operator that applies a transformation on data passing through it.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/readme.js#L24-L29>
pub fn map<I: 'static, O: 'static, F: 'static>(f: F) -> Box<dyn Fn(Source<I>) -> Source<O>>
where
    F: Fn(I) -> O + Send + Sync + Clone,
{
    Box::new(move |source| {
        ({
            let f = f.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    source(Message::Handshake(
                        ({
                            let f = f.clone();
                            move |message| match message {
                                Message::Handshake(source) => {
                                    sink(Message::Handshake(
                                        ({
                                            move |message| match message {
                                                Message::Pull => {
                                                    source(Message::Pull);
                                                }
                                                Message::Terminate => {
                                                    source(Message::Terminate);
                                                }
                                                _ => {}
                                            }
                                        })
                                        .into(),
                                    ));
                                }
                                Message::Data(data) => {
                                    sink(Message::Data(f(data)));
                                }
                                Message::Terminate => {
                                    sink(Message::Terminate);
                                }
                                _ => {}
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
