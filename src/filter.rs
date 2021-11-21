use std::sync::{Arc, RwLock};

use crate::{Message, Source};

/// Callbag operator that conditionally lets data pass through.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/readme.js#L23-L36>
pub fn filter<I: 'static, F: 'static>(condition: F) -> Box<dyn Fn(Source<I>) -> Source<I>>
where
    F: Fn(&I) -> bool + Send + Sync + Clone,
{
    Box::new(move |source| {
        ({
            let condition = condition.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    let talkback = Arc::new(RwLock::new(None));
                    source(Message::Handshake(
                        ({
                            let condition = condition.clone();
                            move |message| {
                                let talkback = talkback.clone();
                                match message {
                                    Message::Handshake(source) => {
                                        {
                                            let mut talkback = talkback.write().unwrap();
                                            *talkback = Some(source);
                                        }
                                        sink(Message::Handshake(
                                            ({
                                                move |message| match message {
                                                    Message::Pull => {
                                                        let talkback = talkback.read().unwrap();
                                                        let source = talkback.as_ref().unwrap();
                                                        source(Message::Pull);
                                                    }
                                                    Message::Terminate => {
                                                        let talkback = talkback.read().unwrap();
                                                        let source = talkback.as_ref().unwrap();
                                                        source(Message::Terminate);
                                                    }
                                                    _ => {}
                                                }
                                            })
                                            .into(),
                                        ));
                                    }
                                    Message::Data(data) => {
                                        if condition(&data) {
                                            sink(Message::Data(data));
                                        } else {
                                            let talkback = talkback.read().unwrap();
                                            let talkback = talkback.as_ref().unwrap();
                                            talkback(Message::Pull);
                                        }
                                    }
                                    Message::Terminate => {
                                        sink(Message::Terminate);
                                    }
                                    _ => {}
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
