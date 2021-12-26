use arc_swap::ArcSwapOption;
use never::Never;
use std::{
    iter,
    sync::{Arc, RwLock},
};

use callbag_core::{Message, Source};

/// Callbag operator that broadcasts a single source to multiple sinks.
///
/// Does reference counting on sinks and starts the source when the first sink gets connected,
/// similar to [RxJS `.share()`][RxJS `share`].
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/index.js#L1-L32>
///
/// [RxJS `share`]: https://rxjs.dev/api/operators/share
pub fn share<T: 'static, S>(source: S) -> Source<T>
where
    T: Clone,
    S: Into<Arc<Source<T>>>,
{
    let source: Arc<Source<T>> = source.into();
    let sinks = Arc::new(RwLock::new(vec![]));
    let source_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));

    (move |message| {
        let sinks = Arc::clone(&sinks);
        let source_talkback = Arc::clone(&source_talkback);
        if let Message::Handshake(sink) = message {
            {
                let sinks = &mut *sinks.write().unwrap();
                sinks.push(Arc::clone(&sink));
            }

            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let sinks = Arc::clone(&sinks);
                    let source_talkback = Arc::clone(&source_talkback);
                    let sink = Arc::clone(&sink);
                    move |message| match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        }
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        }
                        Message::Pull => {
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(Message::Pull);
                        }
                        Message::Error(_) | Message::Terminate => {
                            {
                                let mut sinks = sinks.write().unwrap();
                                let i = sinks.iter().position({
                                    let sink = Arc::clone(&sink);
                                    move |s| Arc::ptr_eq(s, &sink)
                                });
                                if let Some(i) = i {
                                    sinks.splice(i..i + 1, iter::empty());
                                }
                            }
                            if sinks.read().unwrap().is_empty() {
                                let source_talkback = source_talkback.load();
                                let source_talkback = source_talkback.as_ref().unwrap();
                                source_talkback(Message::Terminate);
                            }
                        }
                    }
                }
                .into(),
            );

            if sinks.read().unwrap().len() == 1 {
                source(Message::Handshake(Arc::new(
                    {
                        move |message: Message<T, Never>| {
                            if let Message::Handshake(source) = message.clone() {
                                source_talkback.store(Some(source));
                                sink(Message::Handshake(Arc::clone(&talkback)));
                            } else {
                                let sinks = {
                                    let sinks = &*sinks.read().unwrap();
                                    sinks.clone()
                                };
                                for s in sinks {
                                    s(message.clone());
                                }
                            }
                            if let Message::Error(_) | Message::Terminate = message {
                                let sinks = &mut *sinks.write().unwrap();
                                sinks.clear();
                            }
                        }
                    }
                    .into(),
                )));
                return;
            }

            sink(Message::Handshake(talkback));
        }
    })
    .into()
}
