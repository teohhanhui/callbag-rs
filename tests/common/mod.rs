use never::Never;

use callbag::Message;

pub type MessagePredicate<I, O> = fn(&Message<I, O>) -> bool;

fn noop(_message: Message<Never, Never>) {}

pub fn never(message: Message<Never, Never>) {
    if let Message::Handshake(sink) = message {
        sink(Message::Handshake(noop.into()));
    }
}
