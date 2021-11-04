use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

use callbag::{from_iter, Message};

/// https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L4-L34
#[test]
fn it_sends_array_items_iterator_to_a_puller_sink() {
    let source = from_iter([10, 20, 30]);

    let downwards_expected_types: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [10, 20, 30];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let talkback = Arc::new(RwLock::new(None));
    source(Message::Handshake(
        (move |message| {
            {
                let mut downwards_expected_types = downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.pop_front().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }

            if let Message::Handshake(source) = message {
                {
                    let mut talkback = talkback.write().unwrap();
                    *talkback = Some(source);
                }
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
                return;
            } else if let Message::Data(data) = message {
                let mut downwards_expected = downwards_expected.write().unwrap();
                let e = downwards_expected.pop_front().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        })
        .into(),
    ));
}

/// https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L68-L97
#[test]
fn it_does_not_blow_up_the_stack_when_iterating_something_huge() {
    #[derive(Clone)]
    struct Gen {
        i: Arc<AtomicUsize>,
    }

    impl Gen {
        fn new(i: Arc<AtomicUsize>) -> Self {
            Gen { i }
        }
    }

    impl Iterator for Gen {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            let i = self.i.load(AtomicOrdering::Acquire);
            if i < 1_000_000 {
                self.i.fetch_add(1, AtomicOrdering::AcqRel);
                Some(i)
            } else {
                None
            }
        }
    }

    let i = Arc::new(AtomicUsize::new(0));
    let gen = Gen::new(i.clone());
    let source = from_iter(gen);

    let talkback = Arc::new(RwLock::new(None));
    let iterated = Arc::new(AtomicBool::new(false));
    source(Message::Handshake(
        ({
            let iterated = iterated.clone();
            move |message| {
                if let Message::Handshake(source) = message {
                    {
                        let mut talkback = talkback.write().unwrap();
                        *talkback = Some(source);
                    }
                    let talkback = talkback.read().unwrap();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                    return;
                } else if let Message::Data(_data) = message {
                    let talkback = talkback.read().unwrap();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                    return;
                } else if let Message::Terminate = message {
                    assert_eq!(
                        i.load(AtomicOrdering::Acquire),
                        1_000_000,
                        "1 million items were iterated"
                    );
                    iterated.store(true, AtomicOrdering::Release);
                    return;
                }
            }
        })
        .into(),
    ));
    assert_eq!(
        iterated.load(AtomicOrdering::Acquire),
        true,
        "iteration happened synchronously"
    );
}

/// https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L99-L129
#[test]
fn it_stops_sending_after_source_completion() {
    let source = from_iter([10, 20, 30]);

    let actual = Arc::new(RwLock::new(vec![]));
    let downwards_expected_types: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));

    let talkback = Arc::new(RwLock::new(None));
    source(Message::Handshake(
        ({
            let actual = actual.clone();
            move |message| {
                {
                    let mut downwards_expected_types = downwards_expected_types.write().unwrap();
                    let et = downwards_expected_types.pop_front().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }

                if let Message::Handshake(source) = message {
                    {
                        let mut talkback = talkback.write().unwrap();
                        *talkback = Some(source);
                    }
                    let talkback = talkback.read().unwrap();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                    return;
                } else if let Message::Data(data) = message {
                    {
                        let mut actual = actual.write().unwrap();
                        actual.push(data);
                    }
                    let talkback = talkback.read().unwrap();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                    talkback(Message::Pull);
                    talkback(Message::Pull);
                }
            }
        })
        .into(),
    ));

    assert_eq!(&actual.read().unwrap()[..], [10]);
}
