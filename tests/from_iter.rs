use std::sync::{Arc, RwLock};

use callbag::{from_iter, Message};

#[test]
fn it_sends_array_items_iterator_to_a_puller_sink() {
    let source = from_iter([10, 20, 30].into_iter());

    let downwards_expected_types: Vec<(fn(&Message<_>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Start(_)), "Message::Start"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::End), "Message::End"),
    ];
    let downwards_expected_types = Arc::new(RwLock::new(downwards_expected_types.into_iter()));
    let downwards_expected = [10, 20, 30];
    let downwards_expected = Arc::new(RwLock::new(downwards_expected.into_iter()));

    let talkback = Arc::new(RwLock::new(None));
    source(Message::Start(
        (move |message| {
            {
                let mut downwards_expected_types = downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.next().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }

            if let Message::Start(sink) = message {
                {
                    let mut talkback = talkback.write().unwrap();
                    *talkback = Some(sink);
                }
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);

                return;
            } else if let Message::Data(data) = message {
                let mut downwards_expected = downwards_expected.write().unwrap();
                let e = downwards_expected.next().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        })
        .into(),
    ));
}
