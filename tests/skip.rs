#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use crate::common::{array_queue, VariantName};
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use callbag::{skip, Message, Source};
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    arc_swap::ArcSwapOption,
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    never::Never,
    std::{
        sync::{
            atomic::AtomicBool,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Arc, RwLock,
        },
        time::Duration,
    },
    tracing::info,
    tracing_futures::Instrument,
};

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
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

/// See <https://github.com/staltz/callbag-skip/blob/698d6b7805c9bcddac038ceff25a0f0362adb25a/test.js#L4-L94>
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
async fn it_skips_from_a_pullable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected = Arc::new(array_queue![
        "Handshake",
        "Pull",
        "Pull",
        "Pull",
        "Pull",
        "Pull",
        "Pull",
    ]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Terminate"]);
    let downwards_expected = Arc::new(array_queue![40, 50]);

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sink_ref = Arc::new(ArcSwapOption::from(None));
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        info!("up: {message:?}");
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
                            sink_ref(Message::Handshake(source));
                            return;
                        } else if let Message::Pull = message {
                        } else {
                            return;
                        }

                        if sent.load(AtomicOrdering::Acquire) == 0 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Data(10));
                                    }
                                })
                                .unwrap();
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 1 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Data(20));
                                    }
                                })
                                .unwrap();
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 2 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Data(30));
                                    }
                                })
                                .unwrap();
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 3 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Data(40));
                                    }
                                })
                                .unwrap();
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 4 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Data(50));
                                    }
                                })
                                .unwrap();
                            return;
                        }
                        if sent.load(AtomicOrdering::Acquire) == 5 {
                            sent.fetch_add(1, AtomicOrdering::AcqRel);
                            nursery
                                .nurse({
                                    let sink_ref = Arc::clone(&sink_ref);
                                    async move {
                                        let sink_ref = sink_ref.load();
                                        let sink_ref = sink_ref.as_ref().unwrap();
                                        sink_ref(Message::Terminate);
                                    }
                                })
                                .unwrap();
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
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                } else if let Message::Data(data) = message {
                    {
                        let e = downwards_expected.pop().unwrap();
                        assert_eq!(data, e, "downwards data is expected: {e}");
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    talkback(Message::Pull);
                }
            })
            .into(),
        )
    };

    let source = make_source();
    let skipped = skip(3)(source);
    let sink = make_sink();
    skipped(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(300), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-skip/blob/698d6b7805c9bcddac038ceff25a0f0362adb25a/test.js#L96-L152>
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
async fn it_skips_an_async_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull", "Pull", "Pull"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Terminate"]);
    let downwards_expected = Arc::new(array_queue![40, 50]);

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message: Message<Never, _>| {
                        info!("up: {message:?}");
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert_eq!(message.variant_name(), e, "upwards type is expected: {e}");
                        }
                        if let Message::Handshake(sink) = message {
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        sink(Message::Data(10));
                                    }
                                })
                                .unwrap();
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(200);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        sink(Message::Data(20));
                                    }
                                })
                                .unwrap();
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(300);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        sink(Message::Data(30));
                                    }
                                })
                                .unwrap();
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(400);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        sink(Message::Data(40));
                                    }
                                })
                                .unwrap();
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(500);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        sink(Message::Data(50));
                                        sink(Message::Terminate);
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

    let source = make_source();
    let skipped = skip(3)(source);
    skipped(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(900), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-skip/blob/698d6b7805c9bcddac038ceff25a0f0362adb25a/test.js#L154-L218>
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

    let upwards_expected = Arc::new(array_queue![
        "Handshake",
        "Pull",
        "Pull",
        "Pull",
        "Terminate",
    ]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data"]);
    let downwards_expected = Arc::new(array_queue![40, 50]);

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

    let source = make_source();
    let skipped = skip(3)(source);
    let sink = make_sink();
    skipped(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(800), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
