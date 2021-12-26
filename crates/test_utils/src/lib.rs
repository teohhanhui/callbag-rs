use arc_swap::ArcSwapOption;
use never::Never;
use std::sync::Arc;

use callbag_core::{Callbag, Message};

pub type MessagePredicate<I, O> = fn(&Message<I, O>) -> bool;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum MessageDirection {
    FromUp,
    FromDown,
}

/// See <https://github.com/Andarist/callbag-never/blob/cc7e20b707c597de4c0013b08b3f13baa5553544/src/index.js#L1>
fn noop(_message: Message<Never, Never>) {}

/// See <https://github.com/Andarist/callbag-never/blob/cc7e20b707c597de4c0013b08b3f13baa5553544/src/index.js#L3-L6>
pub fn never(message: Message<Never, Never>) {
    if let Message::Handshake(sink) = message {
        sink(Message::Handshake(Arc::new(noop.into())));
    }
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L540-L555>
pub fn make_mock_callbag<I: 'static, O: 'static, R: 'static>(
    name: &'static str,
    report: R,
    is_source: bool,
) -> (Callbag<I, O>, impl Fn(Message<O, I>))
where
    I: Clone,
    O: Clone,
    R: Fn(&'static str, MessageDirection, Message<I, O>) + Send + Sync,
{
    let report = Arc::new(report);
    let talkback_ref = Arc::new(ArcSwapOption::from(None));
    let mock = {
        let talkback_ref = Arc::clone(&talkback_ref);
        move |message: Message<I, O>| {
            report(name, MessageDirection::FromUp, message.clone());
            if let Message::Handshake(talkback) = message {
                talkback_ref.store(Some(Arc::clone(&talkback)));
                if is_source {
                    let talkback_ref = talkback_ref.load();
                    let talkback = talkback_ref.as_ref().unwrap();
                    talkback(Message::Handshake(Arc::new(
                        {
                            let report = report.clone();
                            move |message| {
                                report(name, MessageDirection::FromDown, message);
                            }
                        }
                        .into(),
                    )));
                }
            }
        }
    };
    let emit = move |message| {
        let talkback_ref = talkback_ref.load();
        let talkback = talkback_ref.as_ref().unwrap();
        talkback(message);
    };
    (mock.into(), emit)
}
