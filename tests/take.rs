use arc_swap::ArcSwapOption;
use never::Never;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{take, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        sync::{atomic::AtomicBool, RwLock},
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

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L4-L103>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
async fn it_takes_from_a_pullable_source() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue![
        "Handshake",
        "Pull",
        "Pull",
        "Pull",
        "Terminate",
    ]);
    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![10, 20, 30]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
        let nursery = nursery.clone();
        move || {
            let sink_ref = Arc::new(ArcSwapOption::from(None));
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
                            sink_ref.store(Some(sink));
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            let source = {
                                let source_ref = &mut *source_ref.write().unwrap();
                                source_ref.take().unwrap()
                            };
                            call!(sink_ref, Message::Handshake(source), "to sink: {message:?}");
                            return;
                        } else if let Message::Pull = message {
                        } else {
                            return;
                        }

                        if sent.load(AtomicOrdering::Acquire) == 0 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
                                nursery
                                    .nurse({
                                        let sink_ref = Arc::clone(&sink_ref);
                                        async move {
                                            let sink_ref = sink_ref.load();
                                            let sink_ref = sink_ref.as_ref().unwrap();
                                            call!(
                                                sink_ref,
                                                Message::Data(10),
                                                "to sink: {message:?}"
                                            );
                                        }
                                    })
                                    .unwrap();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 1 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
                                nursery
                                    .nurse({
                                        let sink_ref = Arc::clone(&sink_ref);
                                        async move {
                                            let sink_ref = sink_ref.load();
                                            let sink_ref = sink_ref.as_ref().unwrap();
                                            call!(
                                                sink_ref,
                                                Message::Data(20),
                                                "to sink: {message:?}"
                                            );
                                        }
                                    })
                                    .unwrap();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 2 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
                                nursery
                                    .nurse({
                                        let sink_ref = Arc::clone(&sink_ref);
                                        async move {
                                            let sink_ref = sink_ref.load();
                                            let sink_ref = sink_ref.as_ref().unwrap();
                                            call!(
                                                sink_ref,
                                                Message::Data(30),
                                                "to sink: {message:?}"
                                            );
                                        }
                                    })
                                    .unwrap();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 3 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
                                nursery
                                    .nurse({
                                        let sink_ref = Arc::clone(&sink_ref);
                                        async move {
                                            let sink_ref = sink_ref.load();
                                            let sink_ref = sink_ref.as_ref().unwrap();
                                            call!(
                                                sink_ref,
                                                Message::Data(40),
                                                "to sink: {message:?}"
                                            );
                                        }
                                    })
                                    .unwrap();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 4 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            {
                                let nursery = nursery
                                    .clone()
                                    .instrument(info_span!(parent: &source_span, "async_task"));
                                nursery
                                    .nurse({
                                        let sink_ref = Arc::clone(&sink_ref);
                                        async move {
                                            let sink_ref = sink_ref.load();
                                            let sink_ref = sink_ref.as_ref().unwrap();
                                            call!(
                                                sink_ref,
                                                Message::Data(50),
                                                "to sink: {message:?}"
                                            );
                                        }
                                    })
                                    .unwrap();
                            }
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 5 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Terminate, "to sink: {message:?}");
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
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "from source: {message:?}");
                } else if let Message::Data(data) = message {
                    {
                        let e = downwards_expected.pop().unwrap();
                        assert_eq!(data, e, "downwards data is expected: {e}");
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "from source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = make_source();
    let taken = take(3)(source);
    let sink = make_sink();
    call!(taken, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(300), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L105-L155>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
async fn it_takes_an_async_listenable_source() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake", "Terminate"]);
    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
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

    let source = make_source();
    let taken = take(3)(source);
    call!(taken, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L157-L216>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
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
                    assert_eq!(data, e, "downwards data is expected: {e}");
                }
                if downwards_expected.is_empty() {
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Terminate, "from source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = make_source();
    let taken = take(9)(source);
    let sink = make_sink();
    call!(taken, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L218-L283>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_does_not_redundantly_terminate_a_synchronous_pullable_source() {
    let test_fn_span = Span::current();

    let terminations = Arc::new(AtomicUsize::new(0));

    let upwards_expected = Arc::new(array_queue!["Pull", "Pull", "Pull", "Terminate"]);
    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![7, 8, 9]);

    let range_infinite = {
        let test_fn_span = test_fn_span.clone();
        let terminations = Arc::clone(&terminations);
        move |start| {
            Arc::new(
                (move |message| {
                    instrument!(parent: &test_fn_span, "range_infinite", range_infinite_span);
                    info!("from sink: {message:?}");
                    let terminations = Arc::clone(&terminations);
                    let upwards_expected = Arc::clone(&upwards_expected);
                    if let Message::Handshake(sink) = message {
                        let counter = Arc::new(AtomicUsize::new(start));
                        call!(
                            sink,
                            Message::Handshake(Arc::new(
                                {
                                    let range_infinite_span = range_infinite_span.clone();
                                    let sink = Arc::clone(&sink);
                                    move |message: Message<Never, _>| {
                                        instrument!(parent: &range_infinite_span, "sink_talkback");
                                        info!("from sink: {message:?}");
                                        {
                                            let e = upwards_expected.pop().unwrap();
                                            assert_eq!(
                                                message.variant_name(),
                                                e,
                                                "upwards type is expected: {e}"
                                            );
                                        }
                                        if let Message::Pull = message {
                                            let counter =
                                                counter.fetch_add(1, AtomicOrdering::AcqRel);
                                            call!(
                                                sink,
                                                Message::Data(counter),
                                                "to sink: {message:?}"
                                            );
                                        } else if let Message::Error(_) | Message::Terminate =
                                            message
                                        {
                                            terminations.fetch_add(1, AtomicOrdering::AcqRel);
                                        }
                                    }
                                }
                                .into(),
                            )),
                            "to sink: {message:?}"
                        );
                    }
                })
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
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "from source: {message:?}");
                } else if let Message::Data(data) = message {
                    {
                        let e = downwards_expected.pop().unwrap();
                        assert_eq!(data, e, "downwards data is expected: {e}");
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "from source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = range_infinite(7);
    let taken = take(3)(source);
    let sink = make_sink();
    call!(taken, Message::Handshake(sink), "to source: {message:?}");

    assert_eq!(
        terminations.load(AtomicOrdering::Acquire),
        1,
        "only 1 source termination happened"
    );
}

/// See <https://github.com/staltz/callbag-take/blob/6ae7755ea5f014306704450a40eb72ffdb21d308/test.js#L285-L351>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_does_not_mutually_terminate_a_sink() {
    let test_fn_span = Span::current();

    let terminations = Arc::new(AtomicUsize::new(0));
    let logger_terminations = Arc::new(AtomicUsize::new(0));

    let range_infinite = {
        let test_fn_span = test_fn_span.clone();
        let terminations = Arc::clone(&terminations);
        move |start| {
            Arc::new(
                (move |message| {
                    instrument!(parent: &test_fn_span, "range_infinite", range_infinite_span);
                    info!("from sink: {message:?}");
                    let terminations = Arc::clone(&terminations);
                    if let Message::Handshake(sink) = message {
                        let counter = Arc::new(AtomicUsize::new(start));
                        call!(
                            sink,
                            Message::Handshake(Arc::new(
                                {
                                    let range_infinite_span = range_infinite_span.clone();
                                    let sink = Arc::clone(&sink);
                                    move |message| {
                                        instrument!(parent: &range_infinite_span, "sink_talkback");
                                        info!("from sink: {message:?}");
                                        if let Message::Pull = message {
                                            let counter =
                                                counter.fetch_add(1, AtomicOrdering::AcqRel);
                                            call!(
                                                sink,
                                                Message::Data(counter),
                                                "to sink: {message:?}"
                                            );
                                        } else if let Message::Error(_) | Message::Terminate =
                                            message
                                        {
                                            terminations.fetch_add(1, AtomicOrdering::AcqRel);
                                        }
                                    }
                                }
                                .into(),
                            )),
                            "to sink: {message:?}"
                        );
                    }
                })
                .into(),
            )
        }
    };

    let make_sink = {
        let test_fn_span = test_fn_span.clone();
        move || {
            let talkback = ArcSwapOption::from(None);
            Arc::new(
                (move |message| {
                    instrument!(parent: &test_fn_span, "sink");
                    info!("from source: {message:?}");
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Pull, "from source: {message:?}");
                    } else if let Message::Data(_data) = message {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Pull, "from source: {message:?}");
                    }
                })
                .into(),
            )
        }
    };

    let logger = {
        let logger_terminations = Arc::clone(&logger_terminations);
        move |source: Source<usize>| {
            Arc::new(
                (move |message| {
                    instrument!(parent: &test_fn_span, "logger", logger_span);
                    info!("from sink: {message:?}");
                    let logger_terminations = Arc::clone(&logger_terminations);
                    if let Message::Handshake(sink) = message {
                        let source_talkback: Arc<ArcSwapOption<Source<usize>>> =
                            Arc::new(ArcSwapOption::from(None));
                        let talkback = Arc::new(
                            {
                                let logger_span = logger_span.clone();
                                let logger_terminations = Arc::clone(&logger_terminations);
                                let source_talkback = Arc::clone(&source_talkback);
                                move |message| {
                                    instrument!(parent: &logger_span, "sink_talkback");
                                    info!("from sink: {message:?}");
                                    if let Message::Error(_) | Message::Terminate = message {
                                        logger_terminations.fetch_add(1, AtomicOrdering::AcqRel);
                                    }
                                    let source_talkback = source_talkback.load();
                                    let source_talkback = source_talkback.as_ref().unwrap();
                                    call!(source_talkback, message, "to source: {message:?}");
                                }
                            }
                            .into(),
                        );
                        call!(
                            source,
                            Message::Handshake(Arc::new(
                                {
                                    let logger_span = logger_span.clone();
                                    move |message| {
                                        instrument!(parent: &logger_span, "source_talkback");
                                        info!("from source: {message:?}");
                                        if let Message::Error(_) | Message::Terminate = message {
                                            logger_terminations
                                                .fetch_add(1, AtomicOrdering::AcqRel);
                                        } else if let Message::Handshake(source) = message {
                                            source_talkback.store(Some(source));
                                            call!(
                                                sink,
                                                Message::Handshake(Arc::clone(&talkback)),
                                                "to sink: {message:?}"
                                            );
                                        } else {
                                            call!(sink, message, "to sink: {message:?}");
                                        }
                                    }
                                }
                                .into(),
                            )),
                            "to source: {message:?}"
                        );
                    }
                })
                .into(),
            )
        }
    };

    let source = range_infinite(7);
    let taken = take(3)(logger(take(3)(source)));
    let sink = make_sink();
    call!(taken, Message::Handshake(sink), "to source: {message:?}");

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
