use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use never::Never;
use std::sync::{
    atomic::{AtomicBool, Ordering as AtomicOrdering},
    Arc,
};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{combine, Message};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use callbag::Source;
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        sync::{atomic::AtomicUsize, RwLock},
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

/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/test.js#L4-L48>
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
async fn it_combines_1_async_finite_listenable_source() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![(1,), (2,), (3,)]);

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
                assert_eq!(data, e, "downwards data is expected: {e:?}");
            }
        })
        .into(),
    );

    let source = combine!(source_a);
    call!(source, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/test.js#L50-L113>
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
async fn it_combines_2_async_finite_listenable_sources() {
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
    let downwards_expected = Arc::new(array_queue![
        (2, "a"),
        (3, "a"),
        (4, "a"),
        (4, "b"),
        (5, "b"),
    ]);

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
                                            if i == 5 {
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
                move |message| {
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
                                        call!(sink, Message::Data("a"), "to sink: {message:?}");
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
                                        call!(sink, Message::Data("b"), "to sink: {message:?}");
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
                assert_eq!(data, e, "downwards data is expected: {e:?}");
            }
        })
        .into(),
    );

    let source = combine!(source_a, source_b);
    call!(source, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/test.js#L115-L176>
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
    let downwards_expected = Arc::new(array_queue![(10,), (20,), (30,)]);

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

    let source = combine!(make_source());
    let sink = make_sink();
    call!(source, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/test.js#L178-L281>
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
async fn it_combines_two_infinite_listenable_sources() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected_a = Arc::new(array_queue!["Handshake", "Terminate"]);
    let upwards_expected_b = Arc::new(array_queue!["Handshake", "Terminate"]);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
    ]);
    let downwards_expected = Arc::new(array_queue![
        (2, "a"),
        (3, "a"),
        (4, "a"),
        (4, "b"),
        (5, "b"),
    ]);

    let source_a = Arc::new(
        {
            let test_fn_span = test_fn_span.clone();
            let nursery = nursery.clone();
            move |message: Message<Never, _>| {
                instrument!(parent: &test_fn_span, "source_a", source_a_span);
                info!("from sink: {message:?}");
                let upwards_expected_a = Arc::clone(&upwards_expected_a);
                {
                    let e = upwards_expected_a.pop().unwrap();
                    assert_eq!(message.variant_name(), e, "upwards A type is expected: {e}");
                }

                if let Message::Handshake(sink) = message {
                    let i = Arc::new(AtomicUsize::new(0));
                    let interval_cleared = Arc::new(AtomicBool::new(false));
                    {
                        let nursery = nursery
                            .clone()
                            .instrument(info_span!(parent: &source_a_span, "async_task"));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                let interval_cleared = Arc::clone(&interval_cleared);
                                const DURATION: Duration = Duration::from_millis(100);
                                async move {
                                    loop {
                                        nursery.sleep(DURATION).await;
                                        if interval_cleared.load(AtomicOrdering::Acquire) {
                                            break;
                                        }
                                        let i = i.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                                        call!(sink, Message::Data(i), "to sink: {message:?}");
                                    }
                                }
                            })
                            .unwrap();
                    }
                    call!(
                        sink,
                        Message::Handshake(Arc::new(
                            {
                                let source_a_span = source_a_span.clone();
                                move |message: Message<Never, _>| {
                                    instrument!(parent: &source_a_span, "sink_talkback");
                                    info!("from sink: {message:?}");
                                    {
                                        let e = upwards_expected_a.pop().unwrap();
                                        assert_eq!(
                                            message.variant_name(),
                                            e,
                                            "upwards A type is expected: {e}"
                                        );
                                    }
                                    if let Message::Error(_) | Message::Terminate = message {
                                        interval_cleared.store(true, AtomicOrdering::Release);
                                    }
                                }
                            }
                            .into(),
                        )),
                        "to sink: {message:?}"
                    );
                }
            }
        }
        .into(),
    );

    let source_b = Arc::new(
        {
            let test_fn_span = test_fn_span.clone();
            let nursery = nursery.clone();
            move |message: Message<Never, _>| {
                instrument!(
                    parent: &test_fn_span,
                    "source_b",
                    source_b_span
                );
                info!("from sink: {message:?}");
                let upwards_expected_b = Arc::clone(&upwards_expected_b);
                {
                    let e = upwards_expected_b.pop().unwrap();
                    assert_eq!(message.variant_name(), e, "upwards B type is expected: {e}");
                }

                if let Message::Handshake(sink) = message {
                    let timeout_cleared = Arc::new(AtomicBool::new(false));
                    {
                        let nursery = nursery.clone().instrument(info_span!(
                            parent: &source_b_span,
                            "async_task"
                        ));
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                let timeout_cleared = Arc::clone(&timeout_cleared);
                                const DURATION: Duration = Duration::from_millis(230);
                                async move {
                                    let async_task_span = Span::current();
                                    nursery.sleep(DURATION).await;
                                    if timeout_cleared.load(AtomicOrdering::Acquire) {
                                        return;
                                    }
                                    call!(
                                        sink,
                                        Message::Data("a"),
                                        "to sink: {message:?}"
                                    );
                                    {
                                        let nursery = nursery.clone().instrument(info_span!(
                                            parent: &async_task_span,
                                            "async_task"
                                        ));
                                        nursery
                                            .nurse({
                                                let nursery = nursery.clone();
                                                let sink = Arc::clone(&sink);
                                                const DURATION: Duration = Duration::from_millis(230);
                                                async move {
                                                    let async_task_span = Span::current();
                                                    nursery.sleep(DURATION).await;
                                                    if timeout_cleared.load(AtomicOrdering::Acquire) {
                                                        return;
                                                    }
                                                    call!(
                                                        sink,
                                                        Message::Data("b"),
                                                        "to sink: {message:?}"
                                                    );
                                                    {
                                                        let nursery = nursery.clone().instrument(info_span!(
                                                            parent: &async_task_span,
                                                            "async_task"
                                                        ));
                                                        nursery
                                                            .nurse({
                                                                let nursery = nursery.clone();
                                                                let sink = Arc::clone(&sink);
                                                                const DURATION: Duration =
                                                                    Duration::from_millis(230);
                                                                async move {
                                                                    let async_task_span = Span::current();
                                                                    nursery.sleep(DURATION).await;
                                                                    if timeout_cleared
                                                                        .load(AtomicOrdering::Acquire)
                                                                    {
                                                                        return;
                                                                    }
                                                                    call!(
                                                                        sink,
                                                                        Message::Data("c"),
                                                                        "to sink: {message:?}"
                                                                    );
                                                                    {
                                                                        let nursery = nursery.clone().instrument(info_span!(
                                                                            parent: &async_task_span,
                                                                            "async_task"
                                                                        ));
                                                                        nursery
                                                                            .nurse({
                                                                                let nursery = nursery.clone();
                                                                                let sink = Arc::clone(&sink);
                                                                                const DURATION: Duration =
                                                                                    Duration::from_millis(230);
                                                                                async move {
                                                                                    nursery
                                                                                        .sleep(DURATION)
                                                                                        .await;
                                                                                    if timeout_cleared.load(
                                                                                        AtomicOrdering::Acquire,
                                                                                    ) {
                                                                                        return;
                                                                                    }
                                                                                    call!(
                                                                                        sink,
                                                                                        Message::Data("d"),
                                                                                        "to sink: {message:?}"
                                                                                    );
                                                                                }
                                                                            })
                                                                            .unwrap();
                                                                    }
                                                                }
                                                            })
                                                            .unwrap();
                                                    }
                                                }
                                            })
                                            .unwrap();
                                    }
                                }
                            })
                            .unwrap();
                    }
                    call!(
                        sink,
                        Message::Handshake(Arc::new(
                            {
                                let source_b_span = source_b_span.clone();
                                move |message: Message<Never, _>| {
                                    instrument!(parent: &source_b_span, "sink_talkback");
                                    info!("from sink: {message:?}");
                                    {
                                        let e = upwards_expected_b.pop().unwrap();
                                        assert_eq!(
                                            message.variant_name(),
                                            e,
                                            "upwards B type is expected: {e}"
                                        );
                                    }
                                    if let Message::Error(_) | Message::Terminate = message {
                                        timeout_cleared.store(true, AtomicOrdering::Release);
                                    }
                                }
                            }
                            .into(),
                        )),
                        "to sink: {message:?}"
                    );
                }
            }
        }
        .into(),
    );

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

    let source = combine!(source_a, source_b);
    let sink = make_sink();
    call!(source, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(800), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/test.js#L283-L358>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_combines_pullable_sources() {
    let test_fn_span = Span::current();

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);

    let downwards_expected = Arc::new(array_queue![
        ("1", "a"),
        ("2", "a"),
        ("3", "a"),
        ("3", "b"),
        ("3", "c"),
    ]);

    let make_pullable = {
        let test_fn_span = test_fn_span.clone();
        move |values| {
            let values = {
                let q = SegQueue::new();
                for x in values {
                    q.push(x);
                }
                Arc::new(q)
            };
            Arc::new(
                {
                    let test_fn_span = test_fn_span.clone();
                    move |message| {
                        instrument!(parent: &test_fn_span, "source", source_span);
                        info!("from sink: {message:?}");
                        match message {
                            Message::Handshake(sink) => {
                                let completed = Arc::new(AtomicBool::new(false));
                                let terminated = Arc::new(AtomicBool::new(false));
                                call!(
                                    sink,
                                    Message::Handshake(Arc::new(
                                        {
                                            let source_span = source_span.clone();
                                            let values = Arc::clone(&values);
                                            let sink = Arc::clone(&sink);
                                            move |message| {
                                                instrument!(parent: &source_span, "sink_talkback");
                                                info!("from sink: {message:?}");
                                                if completed.load(AtomicOrdering::Acquire) {
                                                    return;
                                                }

                                                if let Message::Pull = message {
                                                    let value = values.pop().unwrap();

                                                    if values.is_empty() {
                                                        completed
                                                            .store(true, AtomicOrdering::Release);
                                                    }

                                                    call!(
                                                        sink,
                                                        Message::Data(value),
                                                        "to sink: {message:?}"
                                                    );

                                                    if completed.load(AtomicOrdering::Acquire)
                                                        && !terminated.load(AtomicOrdering::Acquire)
                                                    {
                                                        call!(
                                                            sink,
                                                            Message::Terminate,
                                                            "to sink: {message:?}"
                                                        );
                                                        terminated
                                                            .store(true, AtomicOrdering::Release);
                                                    }
                                                }
                                            }
                                        }
                                        .into(),
                                    )),
                                    "to sink: {message:?}"
                                );
                            },
                            _ => {
                                unimplemented!();
                            },
                        }
                    }
                }
                .into(),
            )
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

                if let Message::Error(_) | Message::Terminate = message {
                    return;
                } else if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    let e = downwards_expected.pop().unwrap();
                    assert_eq!(data, e, "downwards data is expected: {e:?}");
                }

                let talkback = talkback.load();
                let talkback = talkback.as_ref().unwrap();
                call!(talkback, Message::Pull, "to source: {message:?}");
            })
            .into(),
        )
    };

    let source = combine!(
        make_pullable(["1", "2", "3"]),
        make_pullable(["a", "b", "c"])
    );
    call!(
        source,
        Message::Handshake(make_sink()),
        "to source: {message:?}"
    );
}
