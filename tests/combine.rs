use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use never::Never;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};
use tracing::info;

use crate::common::{array_queue, VariantName};

use callbag::{combine, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        sync::{atomic::AtomicUsize, RwLock},
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
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

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
                assert_eq!(data, e, "downwards data is expected: {e:?}");
            }
        })
        .into(),
    );

    let source = combine!(source_a);
    source(Message::Handshake(sink));

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
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

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
                                        if i == 5 {
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
                move |message| {
                    info!("up (b): {message:?}");
                    if let Message::Handshake(sink) = message {
                        nursery
                            .nurse({
                                let nursery = nursery.clone();
                                let sink = Arc::clone(&sink);
                                const DURATION: Duration = Duration::from_millis(230);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Data("a"));
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
                                    sink(Message::Data("b"));
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
                assert_eq!(data, e, "downwards data is expected: {e:?}");
            }
        })
        .into(),
    );

    let source = combine!(source_a, source_b);
    source(Message::Handshake(sink));

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
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected = Arc::new(array_queue!["Handshake", "Terminate"]);
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data", "Data", "Data"]);
    let downwards_expected = Arc::new(array_queue![(10,), (20,), (30,)]);

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
                    assert_eq!(data, e, "downwards data is expected: {e:?}");
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

    let source = combine!(make_source());
    let sink = make_sink();
    source(Message::Handshake(sink));

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
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

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
            let nursery = nursery.clone();
            move |message: Message<Never, _>| {
                let upwards_expected_a = Arc::clone(&upwards_expected_a);
                info!("up (a): {message:?}");
                {
                    let e = upwards_expected_a.pop().unwrap();
                    assert_eq!(message.variant_name(), e, "upwards A type is expected: {e}");
                }

                if let Message::Handshake(sink) = message {
                    let i = Arc::new(AtomicUsize::new(0));
                    let interval_cleared = Arc::new(AtomicBool::new(false));
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
                                    sink(Message::Data(i));
                                }
                            }
                        })
                        .unwrap();
                    sink(Message::Handshake(Arc::new(
                        (move |message: Message<Never, _>| {
                            info!("up (a): {message:?}");
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
                        })
                        .into(),
                    )));
                }
            }
        }
        .into(),
    );

    let source_b = Arc::new(
        {
            let nursery = nursery.clone();
            move |message: Message<Never, _>| {
                let upwards_expected_b = Arc::clone(&upwards_expected_b);
                info!("up (b): {message:?}");
                {
                    let e = upwards_expected_b.pop().unwrap();
                    assert_eq!(message.variant_name(), e, "upwards B type is expected: {e}");
                }

                if let Message::Handshake(sink) = message {
                    let timeout_cleared = Arc::new(AtomicBool::new(false));
                    nursery
                        .nurse({
                            let nursery = nursery.clone();
                            let sink = Arc::clone(&sink);
                            let timeout_cleared = Arc::clone(&timeout_cleared);
                            const DURATION: Duration = Duration::from_millis(230);
                            async move {
                                nursery.sleep(DURATION).await;
                                if timeout_cleared.load(AtomicOrdering::Acquire) {
                                    return;
                                }
                                sink(Message::Data("a"));
                                nursery
                                    .nurse({
                                        let nursery = nursery.clone();
                                        let sink = Arc::clone(&sink);
                                        const DURATION: Duration = Duration::from_millis(230);
                                        async move {
                                            nursery.sleep(DURATION).await;
                                            if timeout_cleared.load(AtomicOrdering::Acquire) {
                                                return;
                                            }
                                            sink(Message::Data("b"));
                                            nursery
                                                .nurse({
                                                    let nursery = nursery.clone();
                                                    let sink = Arc::clone(&sink);
                                                    const DURATION: Duration =
                                                        Duration::from_millis(230);
                                                    async move {
                                                        nursery.sleep(DURATION).await;
                                                        if timeout_cleared
                                                            .load(AtomicOrdering::Acquire)
                                                        {
                                                            return;
                                                        }
                                                        sink(Message::Data("c"));
                                                        nursery
                                                            .nurse({
                                                                let nursery = nursery.clone();
                                                                let sink = Arc::clone(&sink);
                                                                const DURATION: Duration =
                                                                    Duration::from_millis(230);
                                                                async move {
                                                                    nursery.sleep(DURATION).await;
                                                                    if timeout_cleared.load(
                                                                        AtomicOrdering::Acquire,
                                                                    ) {
                                                                        return;
                                                                    }
                                                                    sink(Message::Data("d"));
                                                                }
                                                            })
                                                            .unwrap();
                                                    }
                                                })
                                                .unwrap();
                                        }
                                    })
                                    .unwrap();
                            }
                        })
                        .unwrap();
                    sink(Message::Handshake(Arc::new(
                        (move |message: Message<Never, _>| {
                            info!("up (b): {message:?}");
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
                        })
                        .into(),
                    )));
                }
            }
        }
        .into(),
    );

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
                    assert_eq!(data, e, "downwards data is expected: {e:?}");
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

    let source = combine!(source_a, source_b);
    let sink = make_sink();
    source(Message::Handshake(sink));

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
        (1, "a"),
        (2, "a"),
        (3, "a"),
        (3, "b"),
        (3, "c"),
    ]);

    fn make_pullable<T: 'static, I>(values: I) -> Arc<Source<T>>
    where
        T: Debug + Send + Sync,
        I: IntoIterator<Item = T>,
    {
        let values = {
            let q = SegQueue::new();
            for x in values {
                q.push(x);
            }
            Arc::new(q)
        };
        Arc::new(
            (move |message| match message {
                Message::Handshake(sink) => {
                    let completed = Arc::new(AtomicBool::new(false));
                    let terminated = Arc::new(AtomicBool::new(false));
                    sink(Message::Handshake(Arc::new(
                        {
                            let values = Arc::clone(&values);
                            let sink = Arc::clone(&sink);
                            move |message| {
                                info!("up: {message:?}");
                                if completed.load(AtomicOrdering::Acquire) {
                                    return;
                                }

                                if let Message::Pull = message {
                                    let value = values.pop().unwrap();

                                    if values.is_empty() {
                                        completed.store(true, AtomicOrdering::Release);
                                    }

                                    sink(Message::Data(value));

                                    if completed.load(AtomicOrdering::Acquire)
                                        && !terminated.load(AtomicOrdering::Acquire)
                                    {
                                        sink(Message::Terminate);
                                        terminated.store(true, AtomicOrdering::Release);
                                    }
                                }
                            }
                        }
                        .into(),
                    )));
                },
                _ => {
                    unimplemented!();
                },
            })
            .into(),
        )
    }

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
                talkback(Message::Pull);
            })
            .into(),
        )
    };

    let source = combine!(make_pullable([1, 2, 3]), make_pullable(["a", "b", "c"]));
    source(Message::Handshake(make_sink()));
}
