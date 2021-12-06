use arc_swap::ArcSwapOption;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

use crate::common::MessagePredicate;

use callbag::{take, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use callbag::CallbagFn;
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_nursery::{NurseExt, Nursery},
    futures_timer::Delay,
    std::{pin::Pin, sync::atomic::AtomicBool, time::Duration},
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
use wasm_bindgen_test::wasm_bindgen_test_configure;

pub mod common;

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
wasm_bindgen_test_configure!(run_in_browser);

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L4-L103>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_takes_from_a_pullable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
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

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sink_ref = Arc::new(ArcSwapOption::from(None));
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
            let source = {
                let source_ref = source_ref.clone();
                move |message| {
                    {
                        let upwards_expected = &mut *upwards_expected.write().unwrap();
                        let e = upwards_expected.pop_front().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }

                    if let Message::Handshake(sink) = message {
                        sink_ref.store(Some(Arc::new(sink)));
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        let source = {
                            let source_ref = &mut *source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink_ref(Message::Handshake(source.into()));
                        return;
                    } else if let Message::Pull = message {
                    } else {
                        return;
                    }

                    if sent.load(AtomicOrdering::Acquire) == 0 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        nursery
                            .clone()
                            .nurse({
                                let sink_ref = sink_ref.clone();
                                async move {
                                    let sink_ref = sink_ref.load();
                                    let sink_ref = sink_ref.as_ref().unwrap();
                                    sink_ref(Message::Data(10));
                                }
                            })
                            .unwrap();
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 1 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        nursery
                            .clone()
                            .nurse({
                                let sink_ref = sink_ref.clone();
                                async move {
                                    let sink_ref = sink_ref.load();
                                    let sink_ref = sink_ref.as_ref().unwrap();
                                    sink_ref(Message::Data(20));
                                }
                            })
                            .unwrap();
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 2 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        nursery
                            .clone()
                            .nurse({
                                let sink_ref = sink_ref.clone();
                                async move {
                                    let sink_ref = sink_ref.load();
                                    let sink_ref = sink_ref.as_ref().unwrap();
                                    sink_ref(Message::Data(30));
                                }
                            })
                            .unwrap();
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 3 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        nursery
                            .clone()
                            .nurse({
                                let sink_ref = sink_ref.clone();
                                async move {
                                    let sink_ref = sink_ref.load();
                                    let sink_ref = sink_ref.as_ref().unwrap();
                                    sink_ref(Message::Data(40));
                                }
                            })
                            .unwrap();
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 4 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        nursery
                            .clone()
                            .nurse({
                                let sink_ref = sink_ref.clone();
                                async move {
                                    let sink_ref = sink_ref.load();
                                    let sink_ref = sink_ref.as_ref().unwrap();
                                    sink_ref(Message::Data(50));
                                }
                            })
                            .unwrap();
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 5 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Terminate);
                    }
                }
            };
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Box::new(source.clone()));
            }
            source
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
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
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {}", e);
                }
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        }
    };

    let source = make_source();
    let taken = take(3)(source.into());
    let sink = make_sink();
    taken(Message::Handshake(sink.into()));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(300), nursery_out)
        .await
        .ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L105-L155>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_takes_an_async_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
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

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let interval_cleared = Arc::new(AtomicBool::new(false));
            let source_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
            let source = {
                let source_ref = source_ref.clone();
                move |message| {
                    let interval_cleared = interval_cleared.clone();
                    {
                        let upwards_expected = &mut *upwards_expected.write().unwrap();
                        let e = upwards_expected.pop_front().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
                        let sink = Arc::new(sink);
                        const DURATION: Duration = Duration::from_millis(100);
                        let mut interval = Delay::new(DURATION);
                        nursery
                            .clone()
                            .nurse({
                                let sent = sent.clone();
                                let sink = sink.clone();
                                async move {
                                    loop {
                                        Pin::new(&mut interval).await;
                                        if interval_cleared.load(AtomicOrdering::Acquire) {
                                            break;
                                        }
                                        interval.reset(DURATION);
                                        let sent = sent.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        sink(Message::Data(sent * 10));
                                    }
                                }
                            })
                            .unwrap();
                        let source = {
                            let source_ref = &mut *source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source.into()));
                    } else if let Message::Error(_) | Message::Terminate = message {
                        interval_cleared.store(true, AtomicOrdering::Release);
                    }
                }
            };
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Box::new(source.clone()));
            }
            source
        }
    };

    let sink = move |message| {
        {
            let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
            let et = downwards_expected_types.pop_front().unwrap();
            assert!(et.0(&message), "downwards type is expected: {}", et.1);
        }
        if let Message::Data(data) = message {
            let downwards_expected = &mut *downwards_expected.write().unwrap();
            let e = downwards_expected.pop_front().unwrap();
            assert_eq!(data, e, "downwards data is expected: {}", e);
        }
    };

    let source = make_source();
    let taken = take(3)(source.into());
    taken(Message::Handshake(sink.into()));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(700), nursery_out)
        .await
        .ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L157-L216>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_returns_a_source_that_disposes_upon_upwards_end() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [10, 20, 30];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let interval_cleared = Arc::new(AtomicBool::new(false));
            let source_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
            let source = {
                let source_ref = source_ref.clone();
                move |message| {
                    let interval_cleared = interval_cleared.clone();
                    {
                        let upwards_expected = &mut *upwards_expected.write().unwrap();
                        let e = upwards_expected.pop_front().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
                        let sink = Arc::new(sink);
                        const DURATION: Duration = Duration::from_millis(100);
                        let mut interval = Delay::new(DURATION);
                        nursery
                            .clone()
                            .nurse({
                                let sent = sent.clone();
                                let sink = sink.clone();
                                async move {
                                    loop {
                                        Pin::new(&mut interval).await;
                                        if interval_cleared.load(AtomicOrdering::Acquire) {
                                            break;
                                        }
                                        interval.reset(DURATION);
                                        let sent = sent.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        sink(Message::Data(sent * 10));
                                    }
                                }
                            })
                            .unwrap();
                        let source = {
                            let source_ref = &mut *source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source.into()));
                    } else if let Message::Error(_) | Message::Terminate = message {
                        interval_cleared.store(true, AtomicOrdering::Release);
                    }
                }
            };
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Box::new(source.clone()));
            }
            source
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
        move |message| {
            {
                let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.pop_front().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }
            if let Message::Handshake(source) = message {
                talkback.store(Some(Arc::new(source)));
            } else if let Message::Data(data) = message {
                let downwards_expected = &mut *downwards_expected.write().unwrap();
                let e = downwards_expected.pop_front().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
            }
            let downwards_expected = &*downwards_expected.read().unwrap();
            if downwards_expected.is_empty() {
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Terminate);
            }
        }
    };

    let source = make_source();
    let taken = take(9)(source.into());
    let sink = make_sink();
    taken(Message::Handshake(sink.into()));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(700), nursery_out)
        .await
        .ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L218-L283>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_does_not_redundantly_terminate_a_synchronous_pullable_source() {
    let terminations = Arc::new(AtomicUsize::new(0));

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [7, 8, 9];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let range_infinite = {
        let terminations = terminations.clone();
        move |start| {
            move |message| {
                let terminations = terminations.clone();
                let upwards_expected = upwards_expected.clone();
                if let Message::Handshake(sink) = message {
                    let sink = Arc::new(sink);
                    let counter = Arc::new(AtomicUsize::new(start));
                    sink(Message::Handshake(
                        ({
                            let sink = sink.clone();
                            move |message| {
                                {
                                    let upwards_expected = &mut *upwards_expected.write().unwrap();
                                    let e = upwards_expected.pop_front().unwrap();
                                    assert!(e.0(&message), "upwards type is expected: {}", e.1);
                                }
                                if let Message::Pull = message {
                                    let counter = counter.fetch_add(1, AtomicOrdering::AcqRel);
                                    sink(Message::Data(counter));
                                } else if let Message::Error(_) | Message::Terminate = message {
                                    terminations.fetch_add(1, AtomicOrdering::AcqRel);
                                }
                            }
                        })
                        .into(),
                    ));
                }
            }
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
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
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {}", e);
                }
                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        }
    };

    let source = range_infinite(7);
    let taken = take(3)(source.into());
    let sink = make_sink();
    taken(Message::Handshake(sink.into()));

    assert_eq!(
        terminations.load(AtomicOrdering::Acquire),
        1,
        "only 1 source termination happened"
    );
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L285-L351>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_does_not_mutually_terminate_a_sink() {
    let terminations = Arc::new(AtomicUsize::new(0));
    let logger_terminations = Arc::new(AtomicUsize::new(0));

    let range_infinite = {
        let terminations = terminations.clone();
        move |start| {
            move |message| {
                let terminations = terminations.clone();
                if let Message::Handshake(sink) = message {
                    let sink = Arc::new(sink);
                    let counter = Arc::new(AtomicUsize::new(start));
                    sink(Message::Handshake(
                        ({
                            let sink = sink.clone();
                            move |message| {
                                if let Message::Pull = message {
                                    let counter = counter.fetch_add(1, AtomicOrdering::AcqRel);
                                    sink(Message::Data(counter));
                                } else if let Message::Error(_) | Message::Terminate = message {
                                    terminations.fetch_add(1, AtomicOrdering::AcqRel);
                                }
                            }
                        })
                        .into(),
                    ));
                }
            }
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
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
            }
        }
    };

    let logger = {
        let logger_terminations = logger_terminations.clone();
        move |source: Source<usize>| {
            move |message| {
                let logger_terminations = logger_terminations.clone();
                if let Message::Handshake(sink) = message {
                    let source_talkback: Arc<ArcSwapOption<Source<usize>>> =
                        Arc::new(ArcSwapOption::from(None));
                    let talkback = {
                        let logger_terminations = logger_terminations.clone();
                        let source_talkback = source_talkback.clone();
                        move |message| {
                            if let Message::Error(_) | Message::Terminate = message {
                                logger_terminations.fetch_add(1, AtomicOrdering::AcqRel);
                            }
                            let source_talkback = source_talkback.load();
                            let source_talkback = source_talkback.as_ref().unwrap();
                            source_talkback(message);
                        }
                    };
                    source(Message::Handshake(
                        (move |message| {
                            if let Message::Error(_) | Message::Terminate = message {
                                logger_terminations.fetch_add(1, AtomicOrdering::AcqRel);
                            } else if let Message::Handshake(source) = message {
                                source_talkback.store(Some(Arc::new(source)));
                                sink(Message::Handshake(talkback.clone().into()));
                            } else {
                                sink(message);
                            }
                        })
                        .into(),
                    ));
                }
            }
        }
    };

    let source = range_infinite(7);
    let taken = take(3)(logger(take(3)(source.into())).into());
    let sink = make_sink();
    taken(Message::Handshake(sink.into()));

    assert_eq!(
        terminations.load(AtomicOrdering::Acquire),
        1,
        "only 1 source termination happened"
    );
    assert_eq!(
        logger_terminations.load(AtomicOrdering::Acquire),
        1,
        "logger only observed 1 termination"
    );
}