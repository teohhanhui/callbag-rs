use arc_swap::ArcSwapOption;
use never::Never;
use std::sync::{Arc, RwLock};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{share, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
            Condvar, Mutex,
        },
        time::Duration,
    },
    tracing::info_span,
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
pub mod utils;

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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake"]);

    let downwards_expected_types_a = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected_a = Arc::new(array_queue![10, 20, 30]);

    let downwards_expected_types_b =
        Arc::new(array_queue!["Handshake", "Data", "Data", "Terminate"]);
    let downwards_expected_b = Arc::new(array_queue![20, 30]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        instrument!(parent: &test_fn_span, "source", source_span);
                        info!("from sink: {message:?}");
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
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
                                                    call!(
                                                        sink,
                                                        Message::Data(10),
                                                        "to sink: {message:?}"
                                                    );
                                                    continue;
                                                }
                                                if sent.load(AtomicOrdering::Acquire) == 1 {
                                                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                    call!(
                                                        sink,
                                                        Message::Data(20),
                                                        "to sink: {message:?}"
                                                    );
                                                    continue;
                                                }
                                                if sent.load(AtomicOrdering::Acquire) == 2 {
                                                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                    call!(
                                                        sink,
                                                        Message::Data(30),
                                                        "to sink: {message:?}"
                                                    );
                                                    continue;
                                                }
                                                if sent.load(AtomicOrdering::Acquire) == 3 {
                                                    call!(
                                                        sink,
                                                        Message::Terminate,
                                                        "to sink: {message:?}"
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                    })
                                    .unwrap();
                            }
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink, Message::Handshake(source), "to sink: {message:?}");
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
        {
            let test_fn_span = test_fn_span.clone();
            move |message: Message<_, Never>| {
                instrument!(parent: &test_fn_span, "sink_a");
                info!("from source: {message:?}");
                {
                    let et = downwards_expected_types_a.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards A type is expected: {et}"
                    );
                }
                if let Message::Data(data) = message {
                    let e = downwards_expected_a.pop().unwrap();
                    assert_eq!(data, e, "downwards A data is expected: {e}");
                }
            }
        }
        .into(),
    );

    let sink_b = Arc::new(
        (move |message: Message<_, Never>| {
            instrument!(parent: &test_fn_span, "sink_b");
            info!("from source: {message:?}");
            {
                let et = downwards_expected_types_b.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards B type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected_b.pop().unwrap();
                assert_eq!(data, e, "downwards B data is expected: {e}");
            }
        })
        .into(),
    );

    let source = share(make_source());
    call!(source, Message::Handshake(sink_a), "to source: {message:?}");
    nursery
        .nurse({
            let nursery = nursery.clone();
            const DURATION: Duration = Duration::from_millis(150);
            async move {
                nursery.sleep(DURATION).await;
                call!(source, Message::Handshake(sink_b), "to source: {message:?}");
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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    #[allow(clippy::mutex_atomic)]
    let ready_pair = Arc::new((Mutex::new(false), Condvar::new()));

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull", "Pull", "Pull", "Pull"]);

    let downwards_expected_types_a = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected_a = Arc::new(array_queue![10, 20, 30]);

    let downwards_expected_types_b = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected_b = Arc::new(array_queue![10, 20, 30]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        let ready_pair = Arc::clone(&ready_pair);
        move || {
            let sink_ref = Arc::new(ArcSwapOption::from(None));
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        instrument!(parent: &test_fn_span, "source");
                        info!("from sink: {message:?}");
                        assert!(!upwards_expected.is_empty(), "source can be pulled");
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }

                        if let Message::Handshake(sink) = message {
                            sink_ref.store(Some(sink));
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink_ref, Message::Handshake(source), "to sink: {message:?}");
                            {
                                let (lock, cvar) = &*ready_pair;
                                let mut ready = lock.lock().unwrap();
                                *ready = true;
                                cvar.notify_one();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 3 {
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Terminate, "to sink: {message:?}");
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 0 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let (lock, _) = &*ready_pair;
                                let mut ready = lock.lock().unwrap();
                                *ready = false;
                            }
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(10), "to sink: {message:?}");
                            {
                                let (lock, cvar) = &*ready_pair;
                                let mut ready = lock.lock().unwrap();
                                *ready = true;
                                cvar.notify_one();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 1 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let (lock, _) = &*ready_pair;
                                let mut ready = lock.lock().unwrap();
                                *ready = false;
                            }
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(20), "to sink: {message:?}");
                            {
                                let (lock, cvar) = &*ready_pair;
                                let mut ready = lock.lock().unwrap();
                                *ready = true;
                                cvar.notify_one();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 2 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(30), "to sink: {message:?}");
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

    let make_sink_a = {
        let test_fn_span = test_fn_span.clone();
        let nursery = nursery.clone();
        let ready_pair = Arc::clone(&ready_pair);
        move || {
            let talkback = Arc::new(ArcSwapOption::from(None));
            Arc::new(
                (move |message: Message<_, Never>| {
                    instrument!(parent: &test_fn_span, "sink_a", sink_a_span);
                    info!("from source: {message:?}");
                    {
                        let et = downwards_expected_types_a.pop().unwrap();
                        assert_eq!(
                            message.variant_name(),
                            et,
                            "downwards A type is expected: {et}"
                        );
                    }
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &sink_a_span, "async_task"));
                            nursery
                                .nurse({
                                    let ready_pair = Arc::clone(&ready_pair);
                                    let talkback = Arc::clone(&talkback);
                                    async move {
                                        {
                                            let (lock, cvar) = &*ready_pair;
                                            let mut ready = lock.lock().unwrap();
                                            while !*ready {
                                                ready = cvar.wait(ready).unwrap();
                                            }
                                        }
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        call!(talkback, Message::Pull, "to source: {message:?}");
                                    }
                                })
                                .unwrap();
                        }
                    } else if let Message::Data(data) = message {
                        {
                            let e = downwards_expected_a.pop().unwrap();
                            assert_eq!(data, e, "downwards A data is expected: {e}");
                        }
                        if data == 20 {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &sink_a_span, "async_task"));
                            nursery
                                .nurse({
                                    let ready_pair = Arc::clone(&ready_pair);
                                    let talkback = Arc::clone(&talkback);
                                    async move {
                                        {
                                            let (lock, cvar) = &*ready_pair;
                                            let mut ready = lock.lock().unwrap();
                                            while !*ready {
                                                ready = cvar.wait(ready).unwrap();
                                            }
                                        }
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        call!(talkback, Message::Pull, "to source: {message:?}");
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
                (move |message: Message<_, Never>| {
                    instrument!(parent: &test_fn_span, "sink_b", sink_b_span);
                    info!("from source: {message:?}");
                    {
                        let et = downwards_expected_types_b.pop().unwrap();
                        assert_eq!(
                            message.variant_name(),
                            et,
                            "downwards B type is expected: {et}"
                        );
                    }
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                    } else if let Message::Data(data) = message {
                        {
                            let e = downwards_expected_b.pop().unwrap();
                            assert_eq!(data, e, "downwards B data is expected: {e}");
                        }
                        if data == 10 {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &sink_b_span, "async_task"));
                            nursery
                                .nurse({
                                    let ready_pair = Arc::clone(&ready_pair);
                                    let talkback = Arc::clone(&talkback);
                                    async move {
                                        {
                                            let (lock, cvar) = &*ready_pair;
                                            let mut ready = lock.lock().unwrap();
                                            while !*ready {
                                                ready = cvar.wait(ready).unwrap();
                                            }
                                        }
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        call!(talkback, Message::Pull, "to source: {message:?}");
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
    call!(source, Message::Handshake(sink_a), "to source: {message:?}");
    call!(source, Message::Handshake(sink_b), "to source: {message:?}");

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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake", "Terminate"]);

    let downwards_expected_types_a = Arc::new(array_queue!["Handshake", "Data", "Data"]);
    let downwards_expected_a = Arc::new(array_queue![10, 20]);

    let downwards_expected_types_b =
        Arc::new(array_queue!["Handshake", "Data", "Data", "Data", "Data"]);
    let downwards_expected_b = Arc::new(array_queue![10, 20, 30, 40]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let interval_cleared = Arc::new(AtomicBool::new(false));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        instrument!(parent: &test_fn_span, "source", source_span);
                        info!("from sink: {message:?}");
                        let interval_cleared = Arc::clone(&interval_cleared);
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
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
                                                call!(
                                                    sink,
                                                    Message::Data(sent * 10),
                                                    "to sink: {message:?}"
                                                );
                                            }
                                        }
                                    })
                                    .unwrap();
                            }
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink, Message::Handshake(source), "to sink: {message:?}");
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

    let make_sink_a = {
        let test_fn_span = test_fn_span.clone();
        move || {
            let talkback = Arc::new(ArcSwapOption::from(None));
            Arc::new(
                (move |message: Message<_, Never>| {
                    instrument!(parent: &test_fn_span, "sink_a");
                    info!("from source: {message:?}");
                    {
                        let et = downwards_expected_types_a.pop().unwrap();
                        assert_eq!(
                            message.variant_name(),
                            et,
                            "downwards A type is expected: {et}"
                        );
                    }
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                    } else if let Message::Data(data) = message {
                        let e = downwards_expected_a.pop().unwrap();
                        assert_eq!(data, e, "downwards A data is expected: {e}");
                    }
                    if downwards_expected_a.is_empty() {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Terminate, "to source: {message:?}");
                    }
                })
                .into(),
            )
        }
    };

    let make_sink_b = move || {
        let talkback = Arc::new(ArcSwapOption::from(None));
        Arc::new(
            (move |message: Message<_, Never>| {
                instrument!(parent: &test_fn_span, "sink_b");
                info!("from source: {message:?}");
                {
                    let et = downwards_expected_types_b.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards B type is expected: {et}"
                    );
                }
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected_b.pop().unwrap();
                    assert_eq!(data, e, "downwards B data is expected: {e}");
                }
                if downwards_expected_b.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Terminate, "to source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = share(make_source());
    let sink_a = make_sink_a();
    let sink_b = make_sink_b();
    call!(source, Message::Handshake(sink_a), "to source: {message:?}");
    call!(source, Message::Handshake(sink_b), "to source: {message:?}");

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
    let test_fn_span = Span::current();

    let upwards_expected = Arc::new(array_queue!["Handshake"]);

    let downwards_expected_types_a = Arc::new(array_queue!["Handshake", "Data", "Terminate"]);
    let downwards_expected_a = Arc::new(array_queue!["hi"]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        move || {
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        instrument!(parent: &test_fn_span, "source");
                        info!("from sink: {message:?}");
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink, Message::Handshake(source), "to sink: {message:?}");
                            call!(sink, Message::Data("hi"), "to sink: {message:?}");
                            call!(sink, Message::Terminate, "to sink: {message:?}");
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
        (move |message: Message<_, Never>| {
            instrument!(parent: &test_fn_span, "sink_a");
            info!("from source: {message:?}");
            {
                let et = downwards_expected_types_a.pop().unwrap();
                assert_eq!(
                    message.variant_name(),
                    et,
                    "downwards A type is expected: {et}"
                );
            }
            if let Message::Data(data) = message {
                let e = downwards_expected_a.pop().unwrap();
                assert_eq!(data, e, "downwards A data is expected: {e}");
            }
        })
        .into(),
    );

    let source = share(make_source());
    call!(source, Message::Handshake(sink_a), "to source: {message:?}");
}

/// See <https://github.com/staltz/callbag-share/blob/d96748edec631800ec5e606018f519ccaeb8f766/test.js#L340-L383>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_can_share_for_synchronously_requesting_sink() {
    let test_fn_span = Span::current();

    let upwards_expected_types = Arc::new(array_queue!["Handshake", "Pull"]);

    let downwards_expected_types = Arc::new(array_queue!["Handshake"]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        move || {
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, Never>| {
                        instrument!(parent: &test_fn_span, "source");
                        info!("from sink: {message:?}");
                        {
                            let e = upwards_expected_types.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink, Message::Handshake(source), "to sink: {message:?}");
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
            (move |message: Message<Never, Never>| {
                instrument!(parent: &test_fn_span, "sink");
                info!("from source: {message:?}");
                {
                    let et = downwards_expected_types.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
                if let Message::Handshake(source) = message.clone() {
                    talkback.store(Some(source));
                }
                if let Message::Handshake(_) | Message::Data(_) = message {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "to source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = share(make_source());

    call!(
        source,
        Message::Handshake(make_sink()),
        "to source: {message:?}"
    );
}
