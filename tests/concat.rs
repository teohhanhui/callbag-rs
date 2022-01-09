use arc_swap::ArcSwapOption;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};
use tracing::info;

use crate::common::MessagePredicate;

use callbag::{Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    never::Never,
    std::{error::Error, sync::atomic::AtomicBool, time::Duration},
    tracing_futures::Instrument,
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

/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/test.js#L4-L48>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_concats_1_async_finite_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
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

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {:?}", message);
                    if let Message::Handshake(sink) = message {
                        let i = Arc::new(AtomicUsize::new(0));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(100);
                                async move {
                                    loop {
                                        nursery.sleep(DURATION).await;
                                        let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        sink(Message::Data(i));
                                        if i == 3 {
                                            sink(Message::Terminate);
                                            break;
                                        }
                                    }
                                }
                            })
                            .unwrap();
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_a_ref = source_a_ref.write().unwrap();
            *source_a_ref = Some(Arc::clone(&source_a));
        }
        source_a
    };

    let sink = Arc::new(
        (move |message| {
            info!("down: {:?}", message);
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
        })
        .into(),
    );

    let source = callbag::concat!(source_a);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/test.js#L50-L113>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_concats_2_async_finite_listenable_sources() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = ["1", "2", "3", "a", "b"];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {:?}", message);
                    if let Message::Handshake(sink) = message {
                        let i = Arc::new(AtomicUsize::new(0));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(100);
                                async move {
                                    loop {
                                        nursery.sleep(DURATION).await;
                                        let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        sink(Message::Data(format!("{}", i)));
                                        if i == 3 {
                                            sink(Message::Terminate);
                                            break;
                                        }
                                    }
                                }
                            })
                            .unwrap();
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_a_ref = source_a_ref.write().unwrap();
            *source_a_ref = Some(Arc::clone(&source_a));
        }
        source_a
    };

    let source_b = {
        let source_b_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_b = Arc::new(
            {
                let nursery = nursery.clone();
                let source_b_ref = Arc::clone(&source_b_ref);
                move |message: Message<Never, String>| {
                    info!("up (b): {:?}", message);
                    if let Message::Handshake(sink) = message {
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(230);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("a".to_owned()));
                                }
                            })
                            .unwrap();
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(460);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("b".to_owned()));
                                }
                            })
                            .unwrap();
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(550);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_b));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_b_ref = source_b_ref.write().unwrap();
            *source_b_ref = Some(Arc::clone(&source_b));
        }
        source_b
    };

    let sink = Arc::new(
        (move |message| {
            info!("down: {:?}", message);
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
        })
        .into(),
    );

    let source = callbag::concat!(source_a, source_b);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(1_200), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/test.js#L115-L215>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_concats_2_sync_finite_pullable_sources() {
    let upwards_expected_a: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected_a: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(upwards_expected_a.into()));
    let upwards_expected_b: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected_b: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(upwards_expected_b.into()));

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected_types.into()));
    let downwards_expected = ["10", "20", "30", "a", "b"];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let source_a = {
        let sent_a = Arc::new(AtomicUsize::new(0));
        let sink_a = Arc::new(ArcSwapOption::from(None));
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {:?}", message);
                    if let Message::Handshake(sink) = message {
                        sink_a.store(Some(sink));
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        sink_a(Message::Handshake(source_a));
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 3 {
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        sink_a(Message::Terminate);
                    }
                    {
                        let upwards_expected_a = &*upwards_expected_a.read().unwrap();
                        assert!(!upwards_expected_a.is_empty(), "source can be pulled");
                    }
                    {
                        let upwards_expected_a = &mut *upwards_expected_a.write().unwrap();
                        let e = upwards_expected_a.pop_front().unwrap();
                        assert!(e.0(&message), "upwards A type is expected: {}", e.1);
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 0 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        sink_a(Message::Data("10"));
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 1 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        sink_a(Message::Data("20"));
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 2 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        sink_a(Message::Data("30"));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_a_ref = source_a_ref.write().unwrap();
            *source_a_ref = Some(Arc::clone(&source_a));
        }
        source_a
    };

    let source_b = {
        let sent_b = Arc::new(AtomicUsize::new(0));
        let sink_b = Arc::new(ArcSwapOption::from(None));
        let source_b_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_b = Arc::new(
            {
                let source_b_ref = Arc::clone(&source_b_ref);
                move |message| {
                    info!("up (b): {:?}", message);
                    if let Message::Handshake(sink) = message {
                        sink_b.store(Some(sink));
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        sink_b(Message::Handshake(source_b));
                        return;
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 2 {
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        sink_b(Message::Terminate);
                    }
                    {
                        let upwards_expected_b = &*upwards_expected_b.read().unwrap();
                        assert!(!upwards_expected_b.is_empty(), "source can be pulled");
                    }
                    {
                        let upwards_expected_b = &mut *upwards_expected_b.write().unwrap();
                        let e = upwards_expected_b.pop_front().unwrap();
                        assert!(e.0(&message), "upwards B type is expected: {}", e.1);
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 0 {
                        sent_b.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        sink_b(Message::Data("a"));
                        return;
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 1 {
                        sent_b.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        sink_b(Message::Data("b"));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_b_ref = source_b_ref.write().unwrap();
            *source_b_ref = Some(Arc::clone(&source_b));
        }
        source_b
    };

    let sink = {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message| {
                info!("down: {:?}", message);
                {
                    let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                    let et = downwards_expected_types.pop_front().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
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
        )
    };

    let source = callbag::concat!(source_a, source_b);
    source(Message::Handshake(sink));
}

/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/test.js#L217-L279>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_returns_a_source_that_disposes_upon_upwards_end() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

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
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        info!("up: {:?}", message);
                        let interval_cleared = Arc::clone(&interval_cleared);
                        {
                            let upwards_expected = &mut *upwards_expected.write().unwrap();
                            let e = upwards_expected.pop_front().unwrap();
                            assert!(e.0(&message), "upwards type is expected: {}", e.1);
                        }
                        if let Message::Handshake(sink) = message {
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sent = Arc::clone(&sent);
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            if interval_cleared.load(AtomicOrdering::Acquire) {
                                                break;
                                            }
                                            let sent =
                                                sent.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                            sink(Message::Data(sent * 10));
                                        }
                                    }
                                })
                                .unwrap();
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source));
                        } else if let Message::Error(_) | Message::Terminate = message {
                            interval_cleared.store(true, AtomicOrdering::Release);
                        }
                    }
                }
                .into(),
            );
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Arc::clone(&source));
            }
            source
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message| {
                info!("down: {:?}", message);
                {
                    let downwards_expected_types = &mut *downwards_expected_types.write().unwrap();
                    let et = downwards_expected_types.pop_front().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let e = downwards_expected.pop_front().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {:?}", e);
                }
                let downwards_expected = &*downwards_expected.read().unwrap();
                if downwards_expected.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                }
            })
            .into(),
        )
    };

    let source = callbag::concat!(make_source());
    let sink = make_sink();
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-concat/blob/db3ce91a831309057e165f344a87aa1615b4774e/test.js#L333-L381>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_propagates_source_error_to_sink_and_doesnt_subscribe_to_next_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Error(_)), "Message::Error"),
    ];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let limit = Arc::new(AtomicUsize::new(2));
            let value = Arc::new(AtomicUsize::new(42));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let nursery = nursery.clone();
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        info!("up: {:?}", message);
                        if let Message::Handshake(sink) = message {
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let limit = Arc::clone(&limit);
                                    let value = Arc::clone(&value);
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            let value = value.fetch_add(1, AtomicOrdering::AcqRel);
                                            sink(Message::Data(value));

                                            let limit =
                                                limit.fetch_sub(1, AtomicOrdering::AcqRel) - 1;
                                            if limit == 0 {
                                                sink(Message::Error({
                                                    let err: Box<
                                                        dyn Error + Send + Sync + 'static,
                                                    > = "err".into();
                                                    err.into()
                                                }));
                                                break;
                                            }
                                        }
                                    }
                                })
                                .unwrap();

                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source));
                        }
                    }
                }
                .into(),
            );
            {
                let mut source_ref = source_ref.write().unwrap();
                *source_ref = Some(Arc::clone(&source));
            }
            source
        }
    };

    let make_sink = move || {
        Arc::new(
            (move |message| {
                info!("down: {:?}", message);
                {
                    let downwards_expected = &mut *downwards_expected.write().unwrap();
                    let et = downwards_expected.pop_front().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }
            })
            .into(),
        )
    };

    let source = callbag::concat!(make_source(), make_source());
    let sink = make_sink();
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
