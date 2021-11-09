use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

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

use callbag::{map, Message};

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
wasm_bindgen_test_configure!(run_in_browser);

/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/readme.js#L24-L29>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_maps_a_pullable_source() {
    let upwards_expected: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected_types: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [1, 2, 3];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = move || {
        let sink_ref = Arc::new(RwLock::new(None));
        let sent = Arc::new(AtomicUsize::new(0));
        let source_ref: Arc<RwLock<Option<Box<dyn Fn(Message<_, _>) + Send + Sync>>>> =
            Arc::new(RwLock::new(None));
        let source = {
            let source_ref = source_ref.clone();
            move |message| {
                {
                    let upwards_expected = upwards_expected.read().unwrap();
                    assert!(upwards_expected.len() > 0, "source can be pulled");
                }
                {
                    let mut upwards_expected = upwards_expected.write().unwrap();
                    let e = upwards_expected.pop_front().unwrap();
                    assert!(e.0(&message), "upwards type is expected: {}", e.1);
                }

                if let Message::Handshake(sink) = message {
                    {
                        let mut sink_ref = sink_ref.write().unwrap();
                        *sink_ref = Some(sink);
                    }
                    let sink_ref = sink_ref.read().unwrap();
                    let sink_ref = sink_ref.as_ref().unwrap();
                    let source = {
                        let mut source_ref = source_ref.write().unwrap();
                        source_ref.take().unwrap()
                    };
                    sink_ref(Message::Handshake(source.into()));
                    return;
                }
                if sent.load(AtomicOrdering::Acquire) == 3 {
                    let sink_ref = sink_ref.read().unwrap();
                    let sink_ref = sink_ref.as_ref().unwrap();
                    sink_ref(Message::Terminate);
                    return;
                }
                if sent.load(AtomicOrdering::Acquire) == 0 {
                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let sink_ref = sink_ref.read().unwrap();
                    let sink_ref = sink_ref.as_ref().unwrap();
                    sink_ref(Message::Data(10));
                    return;
                }
                if sent.load(AtomicOrdering::Acquire) == 1 {
                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let sink_ref = sink_ref.read().unwrap();
                    let sink_ref = sink_ref.as_ref().unwrap();
                    sink_ref(Message::Data(20));
                    return;
                }
                if sent.load(AtomicOrdering::Acquire) == 2 {
                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let sink_ref = sink_ref.read().unwrap();
                    let sink_ref = sink_ref.as_ref().unwrap();
                    sink_ref(Message::Data(30));
                    return;
                }
            }
        };
        {
            let mut source_ref = source_ref.write().unwrap();
            *source_ref = Some(Box::new(source.clone()));
        }
        source.into()
    };

    let make_sink = move || {
        let talkback = Arc::new(RwLock::new(None));
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
                {
                    let mut downwards_expected = downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {}", e);
                }
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Pull);
            }
        })
        .into()
    };

    let source = make_source();
    let mapped = map(move |x| (x as f32 * 0.1) as usize)(source);
    let sink = make_sink();
    mapped(Message::Handshake(sink));
}

/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/test.js#L88-L154>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_maps_an_async_finite_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let upwards_expected: Vec<(fn(&Message<_, _>) -> bool, &str)> =
        vec![(|m| matches!(m, Message::Handshake(_)), "Message::Handshake")];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected_types: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [1, 2, 3];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Box<dyn Fn(Message<_, _>) + Send + Sync>>>> =
                Arc::new(RwLock::new(None));
            let source = {
                let source_ref = source_ref.clone();
                move |message| {
                    {
                        let mut upwards_expected = upwards_expected.write().unwrap();
                        let e = upwards_expected.pop_front().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
                        let sink = Arc::new(RwLock::new(sink));
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
                                        interval.reset(DURATION);
                                        if sent.load(AtomicOrdering::Acquire) == 0 {
                                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                                            let sink = sink.read().unwrap();
                                            sink(Message::Data(10));
                                            continue;
                                        }
                                        if sent.load(AtomicOrdering::Acquire) == 1 {
                                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                                            let sink = sink.read().unwrap();
                                            sink(Message::Data(20));
                                            continue;
                                        }
                                        if sent.load(AtomicOrdering::Acquire) == 2 {
                                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                                            let sink = sink.read().unwrap();
                                            sink(Message::Data(30));
                                            continue;
                                        }
                                        if sent.load(AtomicOrdering::Acquire) == 3 {
                                            let sink = sink.read().unwrap();
                                            sink(Message::Terminate);
                                            break;
                                        }
                                    }
                                }
                            })
                            .unwrap();
                        let sink = sink.read().unwrap();
                        let source = {
                            let mut source_ref = source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source.into()));
                    }
                }
            };
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Box::new(source.clone()));
            }
            source.into()
        }
    };

    let sink = move |message| {
        {
            let mut downwards_expected_types = downwards_expected_types.write().unwrap();
            let et = downwards_expected_types.pop_front().unwrap();
            assert!(et.0(&message), "downwards type is expected: {}", et.1);
        }
        if let Message::Data(data) = message {
            {
                let mut downwards_expected = downwards_expected.write().unwrap();
                let e = downwards_expected.pop_front().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
            }
        }
    };

    let source = make_source();
    let mapped = map(move |x| (x as f32 * 0.1) as usize)(source);
    mapped(Message::Handshake(sink.into()));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(700), nursery_out)
        .await
        .ok();
}

/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/test.js#L156-L215>
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
    let upwards_expected: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected_types: Vec<(fn(&Message<_, _>) -> bool, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = [1, 2, 3];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicBool::new(false));
            let source_ref: Arc<RwLock<Option<Box<dyn Fn(Message<_, _>) + Send + Sync>>>> =
                Arc::new(RwLock::new(None));
            let source = {
                let source_ref = source_ref.clone();
                move |message| {
                    let completed = completed.clone();
                    {
                        let mut upwards_expected = upwards_expected.write().unwrap();
                        let e = upwards_expected.pop_front().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
                        let sink = Arc::new(RwLock::new(sink));
                        const DURATION: Duration = Duration::from_millis(100);
                        let mut interval = Delay::new(DURATION);
                        nursery
                            .clone()
                            .nurse({
                                let sent = sent.clone();
                                let sink = sink.clone();
                                async move {
                                    while !completed.load(AtomicOrdering::Acquire) {
                                        Pin::new(&mut interval).await;
                                        interval.reset(DURATION);
                                        let sent = sent.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        let sink = sink.read().unwrap();
                                        sink(Message::Data(sent * 10));
                                    }
                                }
                            })
                            .unwrap();
                        let sink = sink.read().unwrap();
                        let source = {
                            let mut source_ref = source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source.into()));
                    } else if let Message::Terminate = message {
                        completed.store(true, AtomicOrdering::Release);
                    }
                }
            };
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Box::new(source.clone()));
            }
            source.into()
        }
    };

    let make_sink = move || {
        let talkback = Arc::new(RwLock::new(None));
        (move |message| {
            {
                let mut downwards_expected_types = downwards_expected_types.write().unwrap();
                let et = downwards_expected_types.pop_front().unwrap();
                assert!(et.0(&message), "downwards type is expected: {}", et.1);
            }
            if let Message::Handshake(source) = message {
                let mut talkback = talkback.write().unwrap();
                *talkback = Some(source);
            } else if let Message::Data(data) = message {
                let mut downwards_expected = downwards_expected.write().unwrap();
                let e = downwards_expected.pop_front().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
            }
            let downwards_expected = downwards_expected.read().unwrap();
            if downwards_expected.len() == 0 {
                let talkback = talkback.read().unwrap();
                let talkback = talkback.as_ref().unwrap();
                talkback(Message::Terminate);
            }
        })
        .into()
    };

    let source = make_source();
    let mapped = map(move |x| (x as f32 * 0.1) as usize)(source);
    let sink = make_sink();
    mapped(Message::Handshake(sink));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(700), nursery_out)
        .await
        .ok();
}
