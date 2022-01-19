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

use callbag::{filter, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{sync::atomic::AtomicBool, time::Duration},
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

/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/test.js#L4-L85>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_filters_a_pullable_source() {
    let test_fn_span = Span::current();

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull", "Pull", "Pull", "Pull"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Terminate"]);
    let downwards_expected = Arc::new(array_queue![1, 3]);

    let make_source = {
        let test_fn_span = test_fn_span.clone();
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
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(1), "to sink: {message:?}");
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 1 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(2), "to sink: {message:?}");
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 2 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            let sink_ref = sink_ref.load();
                            let sink_ref = sink_ref.as_ref().unwrap();
                            call!(sink_ref, Message::Data(3), "to sink: {message:?}");
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

    let source = make_source();
    let filtered = filter(|x| x % 2 == 1)(source);
    let sink = make_sink();
    call!(filtered, Message::Handshake(sink), "to source: {message:?}");
}

/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/test.js#L87-L152>
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
async fn it_filters_an_async_finite_listenable_source() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Terminate"]);
    let downwards_expected = Arc::new(array_queue![1, 3]);

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
                                                        Message::Data(1),
                                                        "to sink: {message:?}"
                                                    );
                                                    continue;
                                                }
                                                if sent.load(AtomicOrdering::Acquire) == 1 {
                                                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                    call!(
                                                        sink,
                                                        Message::Data(2),
                                                        "to sink: {message:?}"
                                                    );
                                                    continue;
                                                }
                                                if sent.load(AtomicOrdering::Acquire) == 2 {
                                                    sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                    call!(
                                                        sink,
                                                        Message::Data(3),
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
    let filtered = filter(|x| x % 2 == 1)(source);
    call!(filtered, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/test.js#L154-L217>
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

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull", "Terminate"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Data"]);
    let downwards_expected = Arc::new(array_queue![1, 3]);

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
                                                    Message::Data(sent),
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
                    call!(talkback, Message::Terminate, "to source: {message:?}");
                }
            })
            .into(),
        )
    };

    let source = make_source();
    let filtered = filter(|x| x % 2 == 1)(source);
    let sink = make_sink();
    call!(filtered, Message::Handshake(sink), "to source: {message:?}");

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
