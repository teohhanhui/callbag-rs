use arc_swap::ArcSwapOption;
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

use callbag::{flatten, map, CallbagFn, Message};

type MessagePredicate<I, O> = fn(&Message<I, O>) -> bool;

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
wasm_bindgen_test_configure!(run_in_browser);

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L6-L70>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_flattens_a_two_layer_async_infinite_listenable_sources() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = ["a1", "a2", "b1", "b2", "b3", "b4"];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let source_outer = {
        let nursery = nursery.clone();
        let source_outer_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
        let source_outer = {
            let source_outer_ref = source_outer_ref.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    let sink = Arc::new(sink);
                    {
                        let timeout = Delay::new(Duration::from_millis(230));
                        nursery
                            .clone()
                            .nurse({
                                let sink = sink.clone();
                                async move {
                                    timeout.await;
                                    sink(Message::Data("a"));
                                }
                            })
                            .unwrap();
                    }
                    {
                        let timeout = Delay::new(Duration::from_millis(460));
                        nursery
                            .clone()
                            .nurse({
                                let sink = sink.clone();
                                async move {
                                    timeout.await;
                                    sink(Message::Data("b"));
                                }
                            })
                            .unwrap();
                    }
                    {
                        let timeout = Delay::new(Duration::from_millis(690));
                        nursery
                            .clone()
                            .nurse({
                                let sink = sink.clone();
                                async move {
                                    timeout.await;
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
                    }
                    let source_outer = {
                        let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                        source_outer_ref.take().unwrap()
                    };
                    sink(Message::Handshake(source_outer.into()));
                }
            }
        };
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Box::new(source_outer.clone()));
        }
        source_outer
    };

    let source_inner = {
        let nursery = nursery.clone();
        move |message| {
            if let Message::Handshake(sink) = message {
                let sink = Arc::new(sink);
                let i = Arc::new(AtomicUsize::new(0));
                let interval_cleared = Arc::new(AtomicBool::new(false));
                const DURATION: Duration = Duration::from_millis(100);
                let mut interval = Delay::new(DURATION);
                nursery
                    .clone()
                    .nurse({
                        let sink = sink.clone();
                        let interval_cleared = interval_cleared.clone();
                        async move {
                            loop {
                                Pin::new(&mut interval).await;
                                if interval_cleared.load(AtomicOrdering::Acquire) {
                                    break;
                                }
                                interval.reset(DURATION);
                                let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                sink(Message::Data(i));
                                if i == 4 {
                                    interval_cleared.store(true, AtomicOrdering::Release);
                                    sink(Message::Terminate);
                                    break;
                                }
                            }
                        }
                    })
                    .unwrap();
                sink(Message::Handshake(
                    (move |message| {
                        let interval_cleared = interval_cleared.clone();
                        if let Message::Error(_) | Message::Terminate = message {
                            interval_cleared.store(true, AtomicOrdering::Release);
                        }
                    })
                    .into(),
                ));
            }
        }
    };

    let sink = move |message| {
        {
            let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
            let et = downwards_expected_types.pop_front().unwrap();
            assert!(et.0(&message), "downwards type is expected: {}", et.1);
        }
        if let Message::Data(data) = message {
            {
                let downwards_expected = &mut *downwards_expected.write().unwrap();
                let e = downwards_expected.pop_front().unwrap();
                assert_eq!(data, e, "downwards data is expected: {}", e);
            }
        }
    };

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(source_inner.clone().into())
    })(source_outer.into()));
    source(Message::Handshake(sink.into()));

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(1200), nursery_out)
        .await
        .ok();
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L72-L179>
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_flattens_a_two_layer_finite_pullable_sources() {
    let upwards_expected_outer: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected_outer: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(upwards_expected_outer.into()));
    let upwards_expected_inner: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected_inner: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(upwards_expected_inner.into()));

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = ["a10", "a20", "a30", "b10", "b20", "b30"];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let outer_source = {
        let outer_sent = Arc::new(AtomicUsize::new(0));
        let outer_sink = Arc::new(ArcSwapOption::from(None));
        let outer_source_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
        let outer_source = {
            let outer_source_ref = outer_source_ref.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    outer_sink.store(Some(Arc::new(sink)));
                    let outer_sink = outer_sink.load();
                    let outer_sink = outer_sink.as_ref().unwrap();
                    let outer_source = {
                        let outer_source_ref = &mut *outer_source_ref.write().unwrap();
                        outer_source_ref.take().unwrap()
                    };
                    outer_sink(Message::Handshake(outer_source.into()));
                    return;
                }
                {
                    let upwards_expected_outer = &*upwards_expected_outer.read().unwrap();
                    assert!(
                        !upwards_expected_outer.is_empty(),
                        "outer source should be pulled"
                    );
                }
                {
                    let upwards_expected_outer = &mut *upwards_expected_outer.write().unwrap();
                    let e = upwards_expected_outer.pop_front().unwrap();
                    assert!(e.0(&message), "outer upwards type is expected: {}", e.1);
                }
                if outer_sent.load(AtomicOrdering::Acquire) == 0 {
                    outer_sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let outer_sink = outer_sink.load();
                    let outer_sink = outer_sink.as_ref().unwrap();
                    outer_sink(Message::Data("a"));
                    return;
                }
                if outer_sent.load(AtomicOrdering::Acquire) == 1 {
                    outer_sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let outer_sink = outer_sink.load();
                    let outer_sink = outer_sink.as_ref().unwrap();
                    outer_sink(Message::Data("b"));
                    return;
                }
                if outer_sent.load(AtomicOrdering::Acquire) == 2 {
                    let outer_sink = outer_sink.load();
                    let outer_sink = outer_sink.as_ref().unwrap();
                    outer_sink(Message::Terminate);
                }
            }
        };
        {
            let mut outer_source_ref = outer_source_ref.write().unwrap();
            *outer_source_ref = Some(Box::new(outer_source.clone()));
        }
        outer_source
    };

    let make_inner_source = move || {
        let inner_sent = Arc::new(AtomicUsize::new(0));
        let inner_sink = Arc::new(ArcSwapOption::from(None));
        let inner_source_ref: Arc<RwLock<Option<CallbagFn<_, _>>>> = Arc::new(RwLock::new(None));
        let inner_source = {
            let inner_source_ref = inner_source_ref.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    inner_sink.store(Some(Arc::new(sink)));
                    let inner_sink = inner_sink.load();
                    let inner_sink = inner_sink.as_ref().unwrap();
                    let inner_source = {
                        let inner_source_ref = &mut *inner_source_ref.write().unwrap();
                        inner_source_ref.take().unwrap()
                    };
                    inner_sink(Message::Handshake(inner_source.into()));
                    return;
                }
                {
                    let upwards_expected_inner = &*upwards_expected_inner.read().unwrap();
                    assert!(
                        !upwards_expected_inner.is_empty(),
                        "inner source should be pulled"
                    );
                }
                {
                    let upwards_expected_inner = &mut *upwards_expected_inner.write().unwrap();
                    let e = upwards_expected_inner.pop_front().unwrap();
                    assert!(e.0(&message), "inner upwards type is expected: {}", e.1);
                }
                if inner_sent.load(AtomicOrdering::Acquire) == 0 {
                    inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let inner_sink = inner_sink.load();
                    let inner_sink = inner_sink.as_ref().unwrap();
                    inner_sink(Message::Data(10));
                    return;
                }
                if inner_sent.load(AtomicOrdering::Acquire) == 1 {
                    inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let inner_sink = inner_sink.load();
                    let inner_sink = inner_sink.as_ref().unwrap();
                    inner_sink(Message::Data(20));
                    return;
                }
                if inner_sent.load(AtomicOrdering::Acquire) == 2 {
                    inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                    let inner_sink = inner_sink.load();
                    let inner_sink = inner_sink.as_ref().unwrap();
                    inner_sink(Message::Data(30));
                    return;
                }
                if inner_sent.load(AtomicOrdering::Acquire) == 3 {
                    let inner_sink = inner_sink.load();
                    let inner_sink = inner_sink.as_ref().unwrap();
                    inner_sink(Message::Terminate);
                }
            }
        };
        {
            let mut inner_source_ref = inner_source_ref.write().unwrap();
            *inner_source_ref = Some(Box::new(inner_source.clone()));
        }
        inner_source
    };

    let sink = {
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

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(make_inner_source.clone()().into())
    })(outer_source.into()));
    source(Message::Handshake(sink.into()));
}
