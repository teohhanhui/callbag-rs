use arc_swap::ArcSwapOption;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
use wasm_bindgen_test::wasm_bindgen_test_configure;

use callbag::{from_iter, Message};

type MessagePredicate<I, O> = fn(&Message<I, O>) -> bool;

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
wasm_bindgen_test_configure!(run_in_browser);

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L4-L34>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_sends_array_items_iterable_to_a_puller_sink() {
    let source = from_iter([10, 20, 30]);

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
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

    let talkback = ArcSwapOption::from(None);
    source(Message::Handshake(
        (move |message| {
            {
                let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.pop_front().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }

            if let Message::Handshake(source) = message {
                talkback.store(Some(Arc::new(source)));
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            } else if let Message::Data(data) = message {
                {
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {}", e);
                }
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        })
        .into(),
    ));
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L36-L66>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_sends_array_entries_iterator_to_a_puller_sink() {
    let source = from_iter(["a", "b", "c"].into_iter().enumerate());

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [(0, "a"), (1, "b"), (2, "c")];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let talkback = ArcSwapOption::from(None);
    source(Message::Handshake(
        (move |message| {
            {
                let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.pop_front().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }

            if let Message::Handshake(source) = message {
                talkback.store(Some(Arc::new(source)));
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            } else if let Message::Data(data) = message {
                {
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {:?}", e);
                }
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        })
        .into(),
    ));
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L68-L97>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
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

    let talkback = ArcSwapOption::from(None);
    let iterated = Arc::new(AtomicBool::new(false));
    source(Message::Handshake(
        ({
            let iterated = iterated.clone();
            move |message| {
                if let Message::Handshake(source) = message {
                    talkback.store(Some(Arc::new(source)));
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                } else if let Message::Data(_data) = message {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                } else if let Message::Error(_) | Message::Terminate = message {
                    assert_eq!(
                        i.load(AtomicOrdering::Acquire),
                        1_000_000,
                        "1 million items were iterated"
                    );
                    iterated.store(true, AtomicOrdering::Release);
                }
            }
        })
        .into(),
    ));
    assert!(
        iterated.load(AtomicOrdering::Acquire),
        "iteration happened synchronously"
    );
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L99-L129>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_stops_sending_after_source_completion() {
    let source = from_iter([10, 20, 30]);

    let actual = Arc::new(RwLock::new(vec![]));
    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));

    let talkback = ArcSwapOption::from(None);
    source(Message::Handshake(
        ({
            let actual = actual.clone();
            move |message| {
                {
                    let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                    let et = downwards_expected_types.pop_front().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }

                if let Message::Handshake(source) = message {
                    talkback.store(Some(Arc::new(source)));
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                } else if let Message::Data(data) = message {
                    {
                        let actual = &mut *actual.write().unwrap();
                        actual.push(data);
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                    talkback(Message::Pull);
                    talkback(Message::Pull);
                }
            }
        })
        .into(),
    ));

    assert_eq!((&*actual.read().unwrap())[..], [10]);
}
