use arc_swap::ArcSwapOption;
use assert_matches::assert_matches;
use crossbeam_queue::{ArrayQueue, SegQueue};
use never::Never;
use std::{
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};
use tracing::info;

use crate::common::{make_mock_callbag, MessageDirection, VariantName};

use callbag::{merge, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{sync::atomic::AtomicBool, time::Duration},
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

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L4-L48>
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
async fn it_merges_one_async_finite_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Terminate"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = [1, 2, 3];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
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
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            {
                let et = downwards_expected_types.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = merge!(source_a);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L50-L108>
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
async fn it_merges_two_async_finite_listenable_sources() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Data", "Terminate"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["1", "2", "a", "3"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
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
                    info!("up (b): {message:?}");
                    if let Message::Handshake(sink) = message {
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(250);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("a".to_owned()));
                                    nursery
                                        .nurse({
                                            let nursery = nursery.clone();
                                            let sink = Arc::clone(&sink);
                                            const DURATION: Duration = Duration::from_millis(250);
                                            async move {
                                                nursery.sleep(DURATION).await;
                                                sink(Message::Terminate);
                                            }
                                        })
                                        .unwrap();
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
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            {
                let et = downwards_expected_types.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = merge!(source_a, source_b);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L110-L168>
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

    let upwards_expected = ["Handshake", "Terminate"];
    let upwards_expected = {
        let q = ArrayQueue::new(upwards_expected.len());
        for v in upwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = ["Handshake", "Data", "Data", "Data"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = [10, 20, 30];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
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
                    move |message: Message<Never, _>| {
                        info!("up: {message:?}");
                        let interval_cleared = Arc::clone(&interval_cleared);
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
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
            (move |message: Message<_, Never>| {
                info!("down: {message:?}");
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {e}");
                }
                if downwards_expected.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Terminate);
                }
            })
            .into(),
        )
    };

    let source = merge!(make_source());
    let sink = make_sink();
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L170-L250>
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
async fn it_errors_when_one_of_the_sources_errors() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected_a = ["Handshake"];
    let upwards_expected_a = {
        let q = ArrayQueue::new(upwards_expected_a.len());
        for v in upwards_expected_a {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let upwards_expected_b = ["Handshake", "Terminate"];
    let upwards_expected_b = {
        let q = ArrayQueue::new(upwards_expected_b.len());
        for v in upwards_expected_b {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Error"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["11", "101", "12", "err"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let make_source_a = {
        let nursery = nursery.clone();
        move || {
            let count = Arc::new(AtomicUsize::new(0));
            let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source_a = Arc::new(
                {
                    let source_a_ref = Arc::clone(&source_a_ref);
                    move |message: Message<Never, _>| {
                        info!("up (a): {message:?}");
                        {
                            let e = upwards_expected_a.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let count = Arc::clone(&count);
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(20);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            let count =
                                                count.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                            sink(Message::Data(format!("{}", count + 10)));
                                            if count < 2 {
                                                continue;
                                            }
                                            sink(Message::Error({
                                                let err: Box<dyn Error + Send + Sync + 'static> =
                                                    "err".into();
                                                err.into()
                                            }));
                                            break;
                                        }
                                    }
                                })
                                .unwrap();
                            let source_a = {
                                let source_a_ref = &mut *source_a_ref.write().unwrap();
                                source_a_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source_a));
                        } else if let Message::Error(_) | Message::Terminate = message {
                            panic!("Errored source should not receive unsubscribing from merge.");
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
        }
    };

    let make_source_b = {
        let nursery = nursery.clone();
        move || {
            let interval_cleared = Arc::new(AtomicBool::new(false));
            let count = Arc::new(AtomicUsize::new(0));
            let source_b_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source_b = Arc::new(
                {
                    let source_b_ref = Arc::clone(&source_b_ref);
                    move |message: Message<Never, _>| {
                        info!("up (b): {message:?}");
                        let interval_cleared = Arc::clone(&interval_cleared);
                        {
                            let e = upwards_expected_b.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let count = Arc::clone(&count);
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(30);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            if interval_cleared.load(AtomicOrdering::Acquire) {
                                                break;
                                            }
                                            let count =
                                                count.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                            sink(Message::Data(format!("{}", count + 100)));
                                        }
                                    }
                                })
                                .unwrap();
                            let source_b = {
                                let source_b_ref = &mut *source_b_ref.write().unwrap();
                                source_b_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source_b));
                        } else if let Message::Error(_) | Message::Terminate = message {
                            interval_cleared.store(true, AtomicOrdering::Release);
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
        }
    };

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message: Message<_, Never>| {
                info!("down: {message:?}");
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {e}");
                } else if let Message::Error(error) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(format!("{}", error), e, "downwards data is expected: {}", e);
                }
            })
            .into(),
        )
    };

    let source = merge!(make_source_a(), make_source_b());
    let sink = make_sink();
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L252-L302>
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
async fn it_greets_the_sink_as_soon_as_the_first_member_source_greets() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Terminate"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["10", "20", "a"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let sink_greeted = Arc::new(AtomicBool::new(false));

    let quick_source = {
        let quick_source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let quick_source = Arc::new(
            {
                let sink_greeted = Arc::clone(&sink_greeted);
                let quick_source_ref = Arc::clone(&quick_source_ref);
                move |message| {
                    info!("up (quick): {message:?}");
                    if let Message::Handshake(sink) = message {
                        assert!(
                            !sink_greeted.load(AtomicOrdering::Acquire),
                            "sink not yet greeted before any member-source greets"
                        );
                        let quick_source = {
                            let quick_source_ref = &mut *quick_source_ref.write().unwrap();
                            quick_source_ref.take().unwrap()
                        };
                        sink(Message::Handshake(quick_source));
                        assert!(
                            sink_greeted.load(AtomicOrdering::Acquire),
                            "sink greeted right after quick member-source greets"
                        );
                        sink(Message::Data("10"));
                        sink(Message::Data("20"));
                        sink(Message::Terminate);
                    }
                }
            }
            .into(),
        );
        {
            let mut quick_source_ref = quick_source_ref.write().unwrap();
            *quick_source_ref = Some(Arc::clone(&quick_source));
        }
        quick_source
    };

    let slow_source = {
        let slow_source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let slow_source = Arc::new(
            {
                let nursery = nursery.clone();
                let slow_source_ref = Arc::clone(&slow_source_ref);
                move |message| {
                    info!("up (slow): {message:?}");
                    if let Message::Handshake(sink) = message {
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let slow_source_ref = Arc::clone(&slow_source_ref);
                                const DURATION: Duration = Duration::from_millis(50);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    let slow_source = {
                                        let slow_source_ref =
                                            &mut *slow_source_ref.write().unwrap();
                                        slow_source_ref.take().unwrap()
                                    };
                                    sink(Message::Handshake(slow_source));
                                    sink(Message::Data("a"));
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
                    }
                }
            }
            .into(),
        );
        {
            let mut slow_source_ref = slow_source_ref.write().unwrap();
            *slow_source_ref = Some(Arc::clone(&slow_source));
        }
        slow_source
    };

    let sink = {
        let sink_greeted = Arc::clone(&sink_greeted);
        Arc::new(
            (move |message: Message<_, Never>| {
                info!("down: {message:?}");
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
                if let Message::Handshake(_source) = message {
                    sink_greeted.store(true, AtomicOrdering::Release);
                } else if let Message::Data(data) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {e}");
                }
            })
            .into(),
        )
    };

    let source = merge!(quick_source, slow_source);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(500), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L304-L348>
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
async fn it_merges_sync_listenable_sources_resilient_to_greet_terminate_race_conditions_part_1() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Terminate"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["10", "20", "a"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                        sink(Message::Data("10"));
                        sink(Message::Data("20"));
                        sink(Message::Terminate);
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
                move |message| {
                    info!("up (b): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_b));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(50);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("a"));
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
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
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            {
                let et = downwards_expected_types.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = merge!(source_a, source_b);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(500), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L350-L394>
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
async fn it_merges_sync_listenable_sources_resilient_to_greet_terminate_race_conditions_part_2() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Terminate"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["10", "20", "a"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                        sink(Message::Data("10"));
                        sink(Message::Data("20"));
                        sink(Message::Terminate);
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
                move |message| {
                    info!("up (b): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_b));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(50);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("a"));
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
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
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            {
                let et = downwards_expected_types.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = merge!(source_b, source_a);
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(500), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L396-L438>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_merges_sync_listenable_sources_resilient_to_greet_error_race_conditions_part_3() {
    let downwards_expected_types = ["Handshake", "Data", "Data", "Error"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["10", "20", "err"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                        sink(Message::Data("10"));
                        sink(Message::Data("20"));
                        sink(Message::Error({
                            let err: Box<dyn Error + Send + Sync + 'static> = "err".into();
                            err.into()
                        }));
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

    let source_b = Arc::new(
        (move |message| {
            info!("up (b): {message:?}");
            if let Message::Handshake(_sink) = message {
                panic!("source_b should not get subscribed.");
            }
        })
        .into(),
    );

    let sink = Arc::new(
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            {
                let et = downwards_expected_types.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            } else if let Message::Error(error) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(format!("{}", error), e, "downwards data is expected: {}", e);
            }
        })
        .into(),
    );

    let source = merge!(source_a, source_b);
    source(Message::Handshake(sink));
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L440-L490>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_merges_sync_listenable_sources_resilient_to_greet_disposal_race_conditions() {
    let downwards_expected_types = ["Handshake", "Data", "Data"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = [10, 20];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    info!("up (a): {message:?}");
                    if let Message::Handshake(sink) = message {
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_a));
                        sink(Message::Data(10));
                        sink(Message::Data(20));
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

    let source_b = Arc::new(
        (move |message| {
            info!("up (b): {message:?}");
            if let Message::Handshake(_sink) = message {
                panic!("source_b should not get subscribed.");
            }
        })
        .into(),
    );

    let make_sink = move || {
        let limit = AtomicUsize::new(2);
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message: Message<_, Never>| {
                info!("down: {message:?}");
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    {
                        let e = downwards_expected.pop().unwrap();
                        assert_eq!(data, e, "downwards data is expected: {e}");
                    }

                    let limit = limit.fetch_sub(1, AtomicOrdering::AcqRel) - 1;
                    if limit == 0 {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Terminate);
                    }
                }
            })
            .into(),
        )
    };

    let source = merge!(source_a, source_b);
    source(Message::Handshake(make_sink()));
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L492-L516>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn all_sources_get_requests_from_sinks() {
    let history = Arc::new(SegQueue::new());
    let report = {
        let history = Arc::clone(&history);
        move |name, dir, message: Message<Never, Never>| {
            if let Message::Handshake(_) = message {
            } else {
                history.push((name, dir, message));
            }
        }
    };

    let (source_1, _) = make_mock_callbag("source_1", report.clone(), true);
    let (source_2, _) = make_mock_callbag("source_2", report.clone(), true);
    let (source_3, _) = make_mock_callbag("source_3", report, true);
    let (sink, sink_emit) = make_mock_callbag("sink", |_, _, _| {}, false);
    let sink = Arc::new(sink);

    merge!(source_1, source_2, source_3)(Message::Handshake(sink));

    sink_emit(Message::Pull);
    sink_emit(Message::Terminate);

    assert_matches!(
        &{
            let mut v = vec![];
            for _i in 0..history.len() {
                v.push(history.pop().unwrap());
            }
            v
        }[..],
        [
            ("source_1", MessageDirection::FromDown, Message::Pull),
            ("source_2", MessageDirection::FromDown, Message::Pull),
            ("source_3", MessageDirection::FromDown, Message::Pull),
            ("source_1", MessageDirection::FromDown, Message::Terminate),
            ("source_2", MessageDirection::FromDown, Message::Terminate),
            ("source_3", MessageDirection::FromDown, Message::Terminate),
        ],
        "sources all get requests from sink"
    );
}

/// See <https://github.com/staltz/callbag-merge/blob/eefc5930dd5dba5197e4b49dc8ce7dae67be0e6b/test.js#L518-L538>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn all_sources_get_subscription_errors_from_sink() {
    let history = Arc::new(SegQueue::new());
    let report = {
        let history = Arc::clone(&history);
        move |name, dir, message: Message<Never, Never>| {
            if let Message::Handshake(_) = message {
            } else {
                history.push((name, dir, message));
            }
        }
    };

    let (source_1, _) = make_mock_callbag("source_1", report.clone(), true);
    let (source_2, _) = make_mock_callbag("source_2", report.clone(), true);
    let (source_3, _) = make_mock_callbag("source_3", report, true);
    let (sink, sink_emit) = make_mock_callbag("sink", |_, _, _| {}, false);
    let sink = Arc::new(sink);

    merge!(source_1, source_2, source_3)(Message::Handshake(sink));

    sink_emit(Message::Error({
        let err: Box<dyn Error + Send + Sync + 'static> = "err".into();
        err.into()
    }));

    // no way to match "err" inside Message::Error(_)
    assert_matches!(
        &{
            let mut v = vec![];
            for _i in 0..history.len() {
                v.push(history.pop().unwrap());
            }
            v
        }[..],
        [
            ("source_1", MessageDirection::FromDown, Message::Error(_)),
            ("source_2", MessageDirection::FromDown, Message::Error(_)),
            ("source_3", MessageDirection::FromDown, Message::Error(_)),
        ],
        "all sources get errors from sink"
    );
}
