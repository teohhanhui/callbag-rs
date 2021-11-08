use std::sync::{Arc, RwLock};

use crate::{Message, Source};

/// Callbag sink that consume both pullable and listenable sources.
///
/// When called on a pullable source, it will iterate through its data. When called on a listenable
/// source, it will observe its data.
///
/// <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/readme.js#L40-L47>
pub fn for_each<T: 'static, F: 'static>(f: F) -> Box<dyn Fn(Source<T>)>
where
    F: Fn(T) + Send + Sync + Clone,
{
    Box::new(move |source| {
        let talkback = Arc::new(RwLock::new(None));
        source(Message::Handshake(
            ({
                let f = f.clone();
                move |message| {
                    if let Message::Handshake(source) = message {
                        {
                            let mut talkback = talkback.write().unwrap();
                            *talkback = Some(source);
                        }
                        let talkback = talkback.read().unwrap();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    } else if let Message::Data(data) = message {
                        f(data);
                        let talkback = talkback.read().unwrap();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    }
                }
            })
            .into(),
        ));
    })
}
