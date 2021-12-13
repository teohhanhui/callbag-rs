use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{Message, Source};

/// Callbag sink that consumes both pullable and listenable sources.
///
/// When called on a pullable source, it will iterate through its data. When called on a listenable
/// source, it will observe its data.
///
/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/readme.js#L40-L47>
pub fn for_each<T: 'static, F: 'static, S>(f: F) -> Box<dyn Fn(S)>
where
    F: Fn(T) + Send + Sync + Clone,
    S: Into<Arc<Source<T>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<T>> = source.into();
        let talkback = ArcSwapOption::from(None);
        source(Message::Handshake(Arc::new(
            {
                let f = f.clone();
                move |message| match message {
                    Message::Handshake(source) => {
                        talkback.store(Some(source));
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    }
                    Message::Data(data) => {
                        f(data);
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    }
                    Message::Pull => {
                        panic!("source must not pull");
                    }
                    Message::Error(_) => {}
                    Message::Terminate => {}
                }
            }
            .into(),
        )));
    })
}
