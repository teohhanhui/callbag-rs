use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use never::Never;
use std::sync::{Arc, RwLock};
use tracing::info;

use crate::common::MessagePredicate;

use callbag::{share, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        time::Duration,
    },
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

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L4-L91>
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
async fn it_shares_an_async_finite_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> =
        vec![(|m| matches!(m, Message::Handshake(_)), "Message::Handshake")];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_a: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types_a = {
        let q = SegQueue::new();
        downwards_expected_types_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_a = [10, 20, 30];
    let downwards_expected_a = {
        let q = SegQueue::new();
        downwards_expected_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_b: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types_b = {
        let q = SegQueue::new();
        downwards_expected_types_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_b = [20, 30];
    let downwards_expected_b = {
        let q = SegQueue::new();
        downwards_expected_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        info!("up: {:?}", message);
                        {
                            let e = upwards_expected.pop().unwrap();
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
                                            if sent.load(AtomicOrdering::Acquire) == 0 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(10));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 1 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(20));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 2 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(30));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 3 {
                                                sink(Message::Terminate);
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

    let sink_a = Arc::new(
        (move |message| {
            info!("down (a): {:?}", message);
            {
                let et = downwards_expected_types_a.pop().unwrap();
                assert!(et.0(&message), "downwards A type is expected: {}", et.1);
            }
            if let Message::Data(data) = message {
                let e = downwards_expected_a.pop().unwrap();
                assert_eq!(data, e, "downwards A data is expected: {}", e);
            }
        })
        .into(),
    );

    let sink_b = Arc::new(
        (move |message| {
            info!("down (b): {:?}", message);
            {
                let et = downwards_expected_types_b.pop().unwrap();
                assert!(et.0(&message), "downwards B type is expected: {}", et.1);
            }
            if let Message::Data(data) = message {
                let e = downwards_expected_b.pop().unwrap();
                assert_eq!(data, e, "downwards B data is expected: {}", e);
            }
        })
        .into(),
    );

    let source = share(make_source());
    source(Message::Handshake(sink_a));
    nursery
        .nurse({
            let nursery = nursery.clone();
            const DURATION: Duration = Duration::from_millis(150);
            async move {
                nursery.sleep(DURATION).await;
                source(Message::Handshake(sink_b));
            }
        })
        .unwrap();

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L93-L203>
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
async fn it_shares_a_pullable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_a: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types_a = {
        let q = SegQueue::new();
        downwards_expected_types_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_a = [10, 20, 30];
    let downwards_expected_a = {
        let q = SegQueue::new();
        downwards_expected_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_b: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types_b = {
        let q = SegQueue::new();
        downwards_expected_types_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_b = [10, 20, 30];
    let downwards_expected_b = {
        let q = SegQueue::new();
        downwards_expected_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let make_source = move || {
        let sink_ref = Arc::new(ArcSwapOption::from(None));
        let sent = Arc::new(AtomicUsize::new(0));
        let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source = Arc::new(
            {
                let source_ref = Arc::clone(&source_ref);
                move |message| {
                    info!("up: {:?}", message);
                    assert!(!upwards_expected.is_empty(), "source can be pulled");
                    {
                        let e = upwards_expected.pop().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }

                    if let Message::Handshake(sink) = message {
                        sink_ref.store(Some(sink));
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        let source = {
                            let source_ref = &mut *source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink_ref(Message::Handshake(source));
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 3 {
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Terminate);
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 0 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data(10));
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 1 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data(20));
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 2 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data(30));
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
    };

    let make_sink_a = {
        let nursery = nursery.clone();
        move || {
            let talkback = Arc::new(ArcSwapOption::from(None));
            Arc::new(
                (move |message| {
                    info!("down (a): {:?}", message);
                    {
                        let et = downwards_expected_types_a.pop().unwrap();
                        assert!(et.0(&message), "downwards A type is expected: {}", et.1);
                    }
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        nursery
                            .nurse({
                                let talkback = Arc::clone(&talkback);
                                async move {
                                    let talkback = talkback.load();
                                    let talkback = talkback.as_ref().unwrap();
                                    talkback(Message::Pull);
                                }
                            })
                            .unwrap();
                    } else if let Message::Data(data) = message {
                        {
                            let e = downwards_expected_a.pop().unwrap();
                            assert_eq!(data, e, "downwards A data is expected: {}", e);
                        }
                        if data == 20 {
                            nursery
                                .nurse({
                                    let talkback = Arc::clone(&talkback);
                                    async move {
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        talkback(Message::Pull);
                                    }
                                })
                                .unwrap();
                        }
                    }
                })
                .into(),
            )
        }
    };

    let make_sink_b = {
        let nursery = nursery.clone();
        move || {
            let talkback = Arc::new(ArcSwapOption::from(None));
            Arc::new(
                (move |message| {
                    info!("down (b): {:?}", message);
                    {
                        let et = downwards_expected_types_b.pop().unwrap();
                        assert!(et.0(&message), "downwards B type is expected: {}", et.1);
                    }
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                    } else if let Message::Data(data) = message {
                        {
                            let e = downwards_expected_b.pop().unwrap();
                            assert_eq!(data, e, "downwards B data is expected: {}", e);
                        }
                        if data == 10 {
                            nursery
                                .nurse({
                                    let talkback = Arc::clone(&talkback);
                                    async move {
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        talkback(Message::Pull);
                                    }
                                })
                                .unwrap();
                        }
                    }
                })
                .into(),
            )
        }
    };

    let source = share(make_source());
    let sink_a = make_sink_a();
    let sink_b = make_sink_b();
    source(Message::Handshake(sink_a));
    source(Message::Handshake(sink_b));

    let nursery_out = nursery.timeout(Duration::from_millis(500), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L205-L293>
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
async fn it_disposes_only_when_last_sink_sends_upwards_end() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_a: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types_a = {
        let q = SegQueue::new();
        downwards_expected_types_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_a = [10, 20];
    let downwards_expected_a = {
        let q = SegQueue::new();
        downwards_expected_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_b: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
    ];
    let downwards_expected_types_b = {
        let q = SegQueue::new();
        downwards_expected_types_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_b = [10, 20, 30, 40];
    let downwards_expected_b = {
        let q = SegQueue::new();
        downwards_expected_b
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

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
                            let e = upwards_expected.pop().unwrap();
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

    let make_sink_a = move || {
        let talkback = Arc::new(ArcSwapOption::from(None));
        Arc::new(
            (move |message| {
                info!("down (a): {:?}", message);
                {
                    let et = downwards_expected_types_a.pop().unwrap();
                    assert!(et.0(&message), "downwards A type is expected: {}", et.1);
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected_a.pop().unwrap();
                    assert_eq!(data, e, "downwards A data is expected: {}", e);
                }
                if downwards_expected_a.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                }
            })
            .into(),
        )
    };

    let make_sink_b = move || {
        let talkback = Arc::new(ArcSwapOption::from(None));
        Arc::new(
            (move |message| {
                info!("down (b): {:?}", message);
                {
                    let et = downwards_expected_types_b.pop().unwrap();
                    assert!(et.0(&message), "downwards B type is expected: {}", et.1);
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected_b.pop().unwrap();
                    assert_eq!(data, e, "downwards B data is expected: {}", e);
                }
                if downwards_expected_b.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                }
            })
            .into(),
        )
    };

    let source = share(make_source());
    let sink_a = make_sink_a();
    let sink_b = make_sink_b();
    source(Message::Handshake(sink_a));
    source(Message::Handshake(sink_b));

    let nursery_out = nursery.timeout(Duration::from_millis(900), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L295-L338>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_can_share_a_sync_finite_listenable_source() {
    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> =
        vec![(|m| matches!(m, Message::Handshake(_)), "Message::Handshake")];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types_a: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Data(_)), "Message::Data"),
        (|m| matches!(m, Message::Terminate), "Message::Terminate"),
    ];
    let downwards_expected_types_a = {
        let q = SegQueue::new();
        downwards_expected_types_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected_a = ["hi"];
    let downwards_expected_a = {
        let q = SegQueue::new();
        downwards_expected_a
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let make_source = move || {
        let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source = Arc::new(
            {
                let source_ref = Arc::clone(&source_ref);
                move |message| {
                    info!("up: {:?}", message);
                    {
                        let e = upwards_expected.pop().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
                        let source = {
                            let source_ref = &mut *source_ref.write().unwrap();
                            source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source));
                        sink(Message::Data("hi"));
                        sink(Message::Terminate);
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
    };

    let sink_a = Arc::new(
        (move |message| {
            info!("down (a): {:?}", message);
            {
                let et = downwards_expected_types_a.pop().unwrap();
                assert!(et.0(&message), "downwards A type is expected: {}", et.1);
            }
            if let Message::Data(data) = message {
                let e = downwards_expected_a.pop().unwrap();
                assert_eq!(data, e, "downwards A data is expected: {}", e);
            }
        })
        .into(),
    );

    let source = share(make_source());
    source(Message::Handshake(sink_a));
}

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L340-L383>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_can_share_for_synchronously_requesting_sink() {
    let upwards_expected_types: Vec<(MessagePredicate<_, Never>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected_types = {
        let q = SegQueue::new();
        upwards_expected_types
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let downwards_expected_types: Vec<(MessagePredicate<_, _>, &str)> =
        vec![(|m| matches!(m, Message::Handshake(_)), "Message::Handshake")];
    let downwards_expected_types = {
        let q = SegQueue::new();
        downwards_expected_types
            .into_iter()
            .for_each(|item| q.push(item));
        Arc::new(q)
    };

    let make_source = move || {
        let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source = Arc::new(
            {
                let source_ref = Arc::clone(&source_ref);
                move |message| {
                    info!("up: {:?}", message);
                    {
                        let e = upwards_expected_types.pop().unwrap();
                        assert!(e.0(&message), "upwards type is expected: {}", e.1);
                    }
                    if let Message::Handshake(sink) = message {
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
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message| {
                info!("down: {:?}", message);
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert!(et.0(&message), "downwards type is expected: {}", et.1);
                }
                if let Message::Handshake(source) = message.clone() {
                    talkback.store(Some(source));
                }
                if let Message::Handshake(_) | Message::Data(_) = message {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                }
            })
            .into(),
        )
    };

    let source = share(make_source());

    source(Message::Handshake(make_sink()));
}
