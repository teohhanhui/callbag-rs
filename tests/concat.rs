use arc_swap::ArcSwapOption;
use never::Never;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc, RwLock,
};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{error::Error, sync::atomic::AtomicBool, time::Duration},
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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![1, 2, 3]);

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let test_fn_span = test_fn_span.clone();
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    instrument!(parent: &test_fn_span, "source_a", source_a_span);
                    info!("from sink: {message:?}");
                    if let Message::Handshake(sink) = message {
                        let i = Arc::new(AtomicUsize::new(0));
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &source_a_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                            call!(sink, Message::Data(i), "to sink: {message:?}");
                                            if i == 3 {
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
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        call!(sink, Message::Handshake(source_a), "to sink: {message:?}");
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
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = callbag::concat!(source_a);
    call!(source, Message::Handshake(sink), "to source: {message:?}");

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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue!["1", "2", "3", "a", "b"]);

    let source_a = {
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let test_fn_span = test_fn_span.clone();
                let nursery = nursery.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    instrument!(parent: &test_fn_span, "source_a", source_a_span);
                    info!("from sink: {message:?}");
                    if let Message::Handshake(sink) = message {
                        let i = Arc::new(AtomicUsize::new(0));
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &source_a_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    async move {
                                        loop {
                                            nursery.sleep(DURATION).await;
                                            let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                            call!(
                                                sink,
                                                Message::Data(format!("{i}")),
                                                "to sink: {message:?}"
                                            );
                                            if i == 3 {
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
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        call!(sink, Message::Handshake(source_a), "to sink: {message:?}");
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
                let test_fn_span = test_fn_span.clone();
                let nursery = nursery.clone();
                let source_b_ref = Arc::clone(&source_b_ref);
                move |message: Message<Never, String>| {
                    instrument!(parent: &test_fn_span, "source_b", source_b_span);
                    info!("from sink: {message:?}");
                    if let Message::Handshake(sink) = message {
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &source_b_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(230);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        call!(
                                            sink,
                                            Message::Data("a".to_owned()),
                                            "to sink: {message:?}"
                                        );
                                    }
                                })
                                .unwrap();
                        }
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &source_b_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(460);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        call!(
                                            sink,
                                            Message::Data("b".to_owned()),
                                            "to sink: {message:?}"
                                        );
                                    }
                                })
                                .unwrap();
                        }
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &source_b_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(550);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        call!(sink, Message::Terminate, "to sink: {message:?}");
                                    }
                                })
                                .unwrap();
                        }
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        call!(sink, Message::Handshake(source_b), "to sink: {message:?}");
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
            if let Message::Data(data) = message {
                let e = downwards_expected.pop().unwrap();
                assert_eq!(data, e, "downwards data is expected: {e}");
            }
        })
        .into(),
    );

    let source = callbag::concat!(source_a, source_b);
    call!(source, Message::Handshake(sink), "to source: {message:?}");

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
    let test_fn_span = Span::current();

    let upwards_expected_a = Arc::new(array_queue!["Pull", "Pull", "Pull", "Pull"]);
    let upwards_expected_b = Arc::new(array_queue!["Pull", "Pull", "Pull"]);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue!["10", "20", "30", "a", "b"]);

    let source_a = {
        let sent_a = Arc::new(AtomicUsize::new(0));
        let sink_a = Arc::new(ArcSwapOption::from(None));
        let source_a_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_a = Arc::new(
            {
                let test_fn_span = test_fn_span.clone();
                let source_a_ref = Arc::clone(&source_a_ref);
                move |message| {
                    instrument!(parent: &test_fn_span, "source_a");
                    info!("from sink: {message:?}");
                    if let Message::Handshake(sink) = message {
                        sink_a.store(Some(sink));
                        let source_a = {
                            let source_a_ref = &mut *source_a_ref.write().unwrap();
                            source_a_ref.take().unwrap()
                        };
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        call!(sink_a, Message::Handshake(source_a), "to sink: {message:?}");
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 3 {
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        call!(sink_a, Message::Terminate, "to sink: {message:?}");
                    }
                    assert!(!upwards_expected_a.is_empty(), "source can be pulled");
                    {
                        let e = upwards_expected_a.pop().unwrap();
                        assert_eq!(message.variant_name(), e, "upwards A type is expected: {e}");
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 0 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        call!(sink_a, Message::Data("10"), "to sink: {message:?}");
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 1 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        call!(sink_a, Message::Data("20"), "to sink: {message:?}");
                        return;
                    }
                    if sent_a.load(AtomicOrdering::Acquire) == 2 {
                        sent_a.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_a = sink_a.load();
                        let sink_a = sink_a.as_ref().unwrap();
                        call!(sink_a, Message::Data("30"), "to sink: {message:?}");
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
                let test_fn_span = test_fn_span.clone();
                let source_b_ref = Arc::clone(&source_b_ref);
                move |message| {
                    instrument!(parent: &test_fn_span, "source_b");
                    info!("from sink: {message:?}");
                    if let Message::Handshake(sink) = message {
                        sink_b.store(Some(sink));
                        let source_b = {
                            let source_b_ref = &mut *source_b_ref.write().unwrap();
                            source_b_ref.take().unwrap()
                        };
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        call!(sink_b, Message::Handshake(source_b), "to sink: {message:?}");
                        return;
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 2 {
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        call!(sink_b, Message::Terminate, "to sink: {message:?}");
                    }
                    assert!(!upwards_expected_b.is_empty(), "source can be pulled");
                    {
                        let e = upwards_expected_b.pop().unwrap();
                        assert_eq!(message.variant_name(), e, "upwards B type is expected: {e}");
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 0 {
                        sent_b.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        call!(sink_b, Message::Data("a"), "to sink: {message:?}");
                        return;
                    }
                    if sent_b.load(AtomicOrdering::Acquire) == 1 {
                        sent_b.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_b = sink_b.load();
                        let sink_b = sink_b.as_ref().unwrap();
                        call!(sink_b, Message::Data("b"), "to sink: {message:?}");
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
            (move |message: Message<_, Never>| {
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
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "to source: {message:?}");
                } else if let Message::Data(data) = message {
                    {
                        let e = downwards_expected.pop().unwrap();
                        assert_eq!(data, e, "downwards data is expected: {e}");
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "to source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = callbag::concat!(source_a, source_b);
    call!(source, Message::Handshake(sink), "to source: {message:?}");
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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake", "Terminate"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Data"]);
    let downwards_expected = Arc::new(array_queue![10, 20, 30]);

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

    let make_sink = move || {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message: Message<_, Never>| {
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
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {e:?}");
                }
                if downwards_expected.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Terminate, "to source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = callbag::concat!(make_source());
    let sink = make_sink();
    call!(source, Message::Handshake(sink), "to source: {message:?}");

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
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let downwards_expected = Arc::new(array_queue!["Handshake", "Data", "Data", "Error"]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        let nursery = nursery.clone();
        move || {
            let limit = Arc::new(AtomicUsize::new(2));
            let value = Arc::new(AtomicUsize::new(42));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let test_fn_span = test_fn_span.clone();
                    let nursery = nursery.clone();
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        instrument!(parent: &test_fn_span, "source", source_span);
                        info!("from sink: {message:?}");
                        if let Message::Handshake(sink) = message {
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
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
                                                let value =
                                                    value.fetch_add(1, AtomicOrdering::AcqRel);
                                                call!(
                                                    sink,
                                                    Message::Data(value),
                                                    "to sink: {message:?}"
                                                );

                                                let limit =
                                                    limit.fetch_sub(1, AtomicOrdering::AcqRel) - 1;
                                                if limit == 0 {
                                                    call!(
                                                        sink,
                                                        Message::Error({
                                                            let err: Box<
                                                                dyn Error + Send + Sync + 'static,
                                                            > = "err".into();
                                                            err.into()
                                                        }),
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

    let make_sink = move || {
        Arc::new(
            (move |message: Message<_, Never>| {
                instrument!(parent: &test_fn_span, "sink");
                info!("from source: {message:?}");
                {
                    let et = downwards_expected.pop().unwrap();
                    assert_eq!(
                        message.variant_name(),
                        et,
                        "downwards type is expected: {et}"
                    );
                }
            })
            .into(),
        )
    };

    let source = callbag::concat!(make_source(), make_source());
    let sink = make_sink();
    call!(source, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
