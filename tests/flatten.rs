use arc_swap::ArcSwapOption;
use crossbeam_queue::ArrayQueue;
use never::Never;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc, RwLock,
};
use tracing::info;

use crate::common::VariantName;

use callbag::{flatten, map, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use crate::common::never;
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::{
        error::Error,
        sync::{atomic::AtomicBool, Condvar, Mutex},
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

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L6-L70>
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
async fn it_flattens_a_two_layer_async_infinite_listenable_sources() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let downwards_expected_types = [
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["a1", "a2", "b1", "b2", "b3", "b4"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_outer = {
        let source_outer_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_outer = Arc::new(
            {
                let nursery = nursery.clone();
                let source_outer_ref = Arc::clone(&source_outer_ref);
                move |message| {
                    info!("up (outer): {message:?}");
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
                                const DURATION: Duration = Duration::from_millis(690);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Terminate);
                                }
                            })
                            .unwrap();
                        let source_outer = {
                            let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                            source_outer_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_outer));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Arc::clone(&source_outer));
        }
        source_outer
    };

    let source_inner = Arc::new(
        {
            let nursery = nursery.clone();
            move |message| {
                info!("up (inner): {message:?}");
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
                                    if i == 4 {
                                        interval_cleared.store(true, AtomicOrdering::Release);
                                        sink(Message::Terminate);
                                        break;
                                    }
                                }
                            }
                        })
                        .unwrap();
                    sink(Message::Handshake(Arc::new(
                        (move |message| {
                            info!("up (inner): {message:?}");
                            let interval_cleared = Arc::clone(&interval_cleared);
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

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(Arc::clone(&source_inner))
    })(source_outer));
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(1_200), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L72-L179>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_flattens_a_two_layer_finite_pullable_sources() {
    let upwards_expected_outer = ["Pull", "Pull", "Pull"];
    let upwards_expected_outer = {
        let q = ArrayQueue::new(upwards_expected_outer.len());
        for v in upwards_expected_outer {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let upwards_expected_inner = [
        "Pull", "Pull", "Pull", "Pull", "Pull", "Pull", "Pull", "Pull",
    ];
    let upwards_expected_inner = {
        let q = ArrayQueue::new(upwards_expected_inner.len());
        for v in upwards_expected_inner {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let downwards_expected_types = [
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["a10", "a20", "a30", "b10", "b20", "b30"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let outer_source = {
        let outer_sent = Arc::new(AtomicUsize::new(0));
        let outer_sink = Arc::new(ArcSwapOption::from(None));
        let outer_source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let outer_source = Arc::new(
            {
                let outer_source_ref = Arc::clone(&outer_source_ref);
                move |message| {
                    info!("up (outer): {message:?}");
                    if let Message::Handshake(sink) = message {
                        outer_sink.store(Some(sink));
                        let outer_sink = outer_sink.load();
                        let outer_sink = outer_sink.as_ref().unwrap();
                        let outer_source = {
                            let outer_source_ref = &mut *outer_source_ref.write().unwrap();
                            outer_source_ref.take().unwrap()
                        };
                        outer_sink(Message::Handshake(outer_source));
                        return;
                    }
                    assert!(
                        !upwards_expected_outer.is_empty(),
                        "outer source should be pulled"
                    );
                    {
                        let e = upwards_expected_outer.pop().unwrap();
                        assert_eq!(
                            message.variant_name(),
                            e,
                            "outer upwards type is expected: {e}"
                        );
                    }
                    if outer_sent.load(AtomicOrdering::Acquire) == 0 {
                        outer_sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let outer_sink = outer_sink.load();
                        let outer_sink = outer_sink.as_ref().unwrap();
                        outer_sink(Message::Data("a"));
                        return;
                    }
                    if outer_sent.load(AtomicOrdering::Acquire) == 1 {
                        outer_sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let outer_sink = outer_sink.load();
                        let outer_sink = outer_sink.as_ref().unwrap();
                        outer_sink(Message::Data("b"));
                        return;
                    }
                    if outer_sent.load(AtomicOrdering::Acquire) == 2 {
                        let outer_sink = outer_sink.load();
                        let outer_sink = outer_sink.as_ref().unwrap();
                        outer_sink(Message::Terminate);
                    }
                }
            }
            .into(),
        );
        {
            let mut outer_source_ref = outer_source_ref.write().unwrap();
            *outer_source_ref = Some(Arc::clone(&outer_source));
        }
        outer_source
    };

    let make_inner_source = move || {
        let inner_sent = Arc::new(AtomicUsize::new(0));
        let inner_sink = Arc::new(ArcSwapOption::from(None));
        let inner_source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let inner_source = Arc::new(
            {
                let inner_source_ref = Arc::clone(&inner_source_ref);
                move |message| {
                    info!("up (inner): {message:?}");
                    if let Message::Handshake(sink) = message {
                        inner_sink.store(Some(sink));
                        let inner_sink = inner_sink.load();
                        let inner_sink = inner_sink.as_ref().unwrap();
                        let inner_source = {
                            let inner_source_ref = &mut *inner_source_ref.write().unwrap();
                            inner_source_ref.take().unwrap()
                        };
                        inner_sink(Message::Handshake(inner_source));
                        return;
                    }
                    assert!(
                        !upwards_expected_inner.is_empty(),
                        "inner source should be pulled"
                    );
                    {
                        let e = upwards_expected_inner.pop().unwrap();
                        assert_eq!(
                            message.variant_name(),
                            e,
                            "inner upwards type is expected: {e}"
                        );
                    }
                    if inner_sent.load(AtomicOrdering::Acquire) == 0 {
                        inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let inner_sink = inner_sink.load();
                        let inner_sink = inner_sink.as_ref().unwrap();
                        inner_sink(Message::Data(10));
                        return;
                    }
                    if inner_sent.load(AtomicOrdering::Acquire) == 1 {
                        inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let inner_sink = inner_sink.load();
                        let inner_sink = inner_sink.as_ref().unwrap();
                        inner_sink(Message::Data(20));
                        return;
                    }
                    if inner_sent.load(AtomicOrdering::Acquire) == 2 {
                        inner_sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let inner_sink = inner_sink.load();
                        let inner_sink = inner_sink.as_ref().unwrap();
                        inner_sink(Message::Data(30));
                        return;
                    }
                    if inner_sent.load(AtomicOrdering::Acquire) == 3 {
                        let inner_sink = inner_sink.load();
                        let inner_sink = inner_sink.as_ref().unwrap();
                        inner_sink(Message::Terminate);
                    }
                }
            }
            .into(),
        );
        {
            let mut inner_source_ref = inner_source_ref.write().unwrap();
            *inner_source_ref = Some(Arc::clone(&inner_source));
        }
        inner_source
    };

    let sink = Arc::new(
        {
            let talkback = ArcSwapOption::from(None);
            move |message: Message<_, Never>| {
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
            }
        }
        .into(),
    );

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(make_inner_source.clone()())
    })(outer_source));
    source(Message::Handshake(sink));
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L247-L318>
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
async fn it_errors_sink_and_unsubscribe_from_inner_when_outer_throws() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let inner_expected_types = ["Handshake", "Handshake", "Terminate"];
    let inner_expected_types = {
        let q = ArrayQueue::new(inner_expected_types.len());
        for v in inner_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = ["Handshake", "Data", "Data", "Data", "Data", "Error"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["a1", "a2", "b1", "b2"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_outer = {
        let source_outer_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_outer = Arc::new(
            {
                let nursery = nursery.clone();
                let source_outer_ref = Arc::clone(&source_outer_ref);
                move |message| {
                    info!("up (outer): {message:?}");
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
                                const DURATION: Duration = Duration::from_millis(690);
                                async move {
                                    nursery.sleep(DURATION).await;
                                    sink(Message::Error({
                                        let err: Box<dyn Error + Send + Sync + 'static> =
                                            "42".into();
                                        err.into()
                                    }));
                                }
                            })
                            .unwrap();
                        let source_outer = {
                            let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                            source_outer_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_outer));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Arc::clone(&source_outer));
        }
        source_outer
    };

    let source_inner = Arc::new(
        {
            let nursery = nursery.clone();
            move |message: Message<Never, _>| {
                info!("up (inner): {message:?}");
                {
                    let et = inner_expected_types.pop().unwrap();
                    assert_eq!(message.variant_name(), et, "inner type is expected: {et}");
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
                                    if i == 4 {
                                        interval_cleared.store(true, AtomicOrdering::Release);
                                        sink(Message::Terminate);
                                        break;
                                    }
                                }
                            }
                        })
                        .unwrap();
                    sink(Message::Handshake(Arc::new(
                        (move |message| {
                            info!("up (inner): {message:?}");
                            let interval_cleared = Arc::clone(&interval_cleared);
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

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(Arc::clone(&source_inner))
    })(source_outer));
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(1_200), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L320-L391>
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
async fn it_errors_sink_and_unsubscribe_from_outer_when_inner_throws() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let outer_expected_types = ["Handshake", "Terminate"];
    let outer_expected_types = {
        let q = ArrayQueue::new(outer_expected_types.len());
        for v in outer_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = [
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Data",
        "Error",
    ];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected = ["a1", "a2", "b1", "b2", "b3", "b4"];
    let downwards_expected = {
        let q = ArrayQueue::new(downwards_expected.len());
        for v in downwards_expected {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_outer = {
        let source_outer_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_outer = Arc::new(
            {
                let nursery = nursery.clone();
                let source_outer_ref = Arc::clone(&source_outer_ref);
                move |message: Message<Never, _>| {
                    info!("up (outer): {message:?}");
                    {
                        let et = outer_expected_types.pop().unwrap();
                        assert_eq!(message.variant_name(), et, "outer type is expected: {et}");
                    }

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
                        let source_outer = {
                            let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                            source_outer_ref.take().unwrap()
                        };
                        sink(Message::Handshake(source_outer));
                    }
                }
            }
            .into(),
        );
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Arc::clone(&source_outer));
        }
        source_outer
    };

    let source_inner = Arc::new(
        {
            let nursery = nursery.clone();
            move |message| {
                info!("up (inner): {message:?}");
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
                                    if i == 4 {
                                        interval_cleared.store(true, AtomicOrdering::Release);
                                        sink(Message::Error({
                                            let err: Box<dyn Error + Send + Sync + 'static> =
                                                "42".into();
                                            err.into()
                                        }));
                                        break;
                                    }
                                }
                            }
                        })
                        .unwrap();
                    sink(Message::Handshake(Arc::new(
                        (move |message| {
                            info!("up (inner): {message:?}");
                            let interval_cleared = Arc::clone(&interval_cleared);
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

    let source = flatten(map(move |str| {
        map(move |num| format!("{}{}", str, num))(Arc::clone(&source_inner))
    })(source_outer));
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(1_200), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L393-L433>
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
async fn it_should_not_try_to_unsubscribe_from_completed_source_when_waiting_for_inner_completion()
{
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();
    #[allow(clippy::mutex_atomic)]
    let ready_pair = Arc::new((Mutex::new(false), Condvar::new()));

    let outer_expected_types = ["Handshake"];
    let outer_expected_types = {
        let q = ArrayQueue::new(outer_expected_types.len());
        for v in outer_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = ["Handshake"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_outer = {
        let source_outer_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_outer = Arc::new(
            {
                let ready_pair = Arc::clone(&ready_pair);
                let source_outer_ref = Arc::clone(&source_outer_ref);
                move |message: Message<Never, _>| {
                    info!("up (outer): {message:?}");
                    {
                        let et = outer_expected_types.pop().unwrap();
                        assert_eq!(message.variant_name(), et, "outer type is expected: {et}");
                    }

                    if let Message::Handshake(sink) = message {
                        {
                            let source_outer = {
                                let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                                source_outer_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source_outer));
                        }
                        sink(Message::Data(true));
                        sink(Message::Terminate);
                        {
                            let (lock, cvar) = &*ready_pair;
                            let mut ready = lock.lock().unwrap();
                            *ready = true;
                            cvar.notify_one();
                        }
                    }
                }
            }
            .into(),
        );
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Arc::clone(&source_outer));
        }
        source_outer
    };

    let sink = Arc::new(
        {
            let nursery = nursery.clone();
            move |message: Message<_, Never>| {
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
                    let talkback = source;
                    nursery
                        .nurse({
                            let ready_pair = Arc::clone(&ready_pair);
                            async move {
                                {
                                    let (lock, cvar) = &*ready_pair;
                                    let mut ready = lock.lock().unwrap();
                                    while !*ready {
                                        ready = cvar.wait(ready).unwrap();
                                    }
                                }
                                talkback(Message::Terminate);
                            }
                        })
                        .unwrap();
                }
            }
        }
        .into(),
    );

    let source = flatten(map(|_| {
        let never: Arc<Source<Never>> = Arc::new(never.into());
        never
    })(source_outer));
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(100), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}

/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/test.js#L435-L480>
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
async fn it_should_not_try_to_unsubscribe_from_completed_source_when_for_inner_errors() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();
    #[allow(clippy::mutex_atomic)]
    let ready_pair = Arc::new((Mutex::new(false), Condvar::new()));

    let outer_expected_types = ["Handshake"];
    let outer_expected_types = {
        let q = ArrayQueue::new(outer_expected_types.len());
        for v in outer_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };
    let downwards_expected_types = ["Handshake", "Error"];
    let downwards_expected_types = {
        let q = ArrayQueue::new(downwards_expected_types.len());
        for v in downwards_expected_types {
            q.push(v).ok();
        }
        Arc::new(q)
    };

    let source_outer = {
        let source_outer_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_outer = Arc::new(
            {
                let ready_pair = Arc::clone(&ready_pair);
                let source_outer_ref = Arc::clone(&source_outer_ref);
                move |message: Message<Never, _>| {
                    info!("up (outer): {message:?}");
                    {
                        let et = outer_expected_types.pop().unwrap();
                        assert_eq!(message.variant_name(), et, "outer type is expected: {et}");
                    }

                    if let Message::Handshake(sink) = message {
                        {
                            let source_outer = {
                                let source_outer_ref = &mut *source_outer_ref.write().unwrap();
                                source_outer_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source_outer));
                        }
                        sink(Message::Data(true));
                        sink(Message::Terminate);
                        {
                            let (lock, cvar) = &*ready_pair;
                            let mut ready = lock.lock().unwrap();
                            *ready = true;
                            cvar.notify_one();
                        }
                    }
                }
            }
            .into(),
        );
        {
            let mut source_outer_ref = source_outer_ref.write().unwrap();
            *source_outer_ref = Some(Arc::clone(&source_outer));
        }
        source_outer
    };

    let source_inner = {
        let source_inner_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source_inner = Arc::new(
            {
                let nursery = nursery.clone();
                let source_inner_ref = Arc::clone(&source_inner_ref);
                move |message: Message<Never, Never>| {
                    info!("up (inner): {message:?}");
                    if let Message::Handshake(sink) = message {
                        {
                            let source_inner = {
                                let source_inner_ref = &mut *source_inner_ref.write().unwrap();
                                source_inner_ref.take().unwrap()
                            };
                            sink(Message::Handshake(source_inner));
                        }
                        nursery
                            .nurse({
                                let ready_pair = Arc::clone(&ready_pair);
                                async move {
                                    {
                                        let (lock, cvar) = &*ready_pair;
                                        let mut ready = lock.lock().unwrap();
                                        while !*ready {
                                            ready = cvar.wait(ready).unwrap();
                                        }
                                    }
                                    sink(Message::Error({
                                        let err: Box<dyn Error + Send + Sync + 'static> =
                                            "true".into();
                                        err.into()
                                    }));
                                }
                            })
                            .unwrap();
                    }
                }
            }
            .into(),
        );
        {
            let mut source_inner_ref = source_inner_ref.write().unwrap();
            *source_inner_ref = Some(Arc::clone(&source_inner));
        }
        source_inner
    };

    let sink = Arc::new(
        (move |message: Message<_, Never>| {
            info!("down: {message:?}");
            let et = downwards_expected_types.pop().unwrap();
            assert_eq!(
                message.variant_name(),
                et,
                "downwards type is expected: {et}"
            );
        })
        .into(),
    );

    let source = flatten(map(move |_| Arc::clone(&source_inner))(source_outer));
    source(Message::Handshake(sink));

    let nursery_out = nursery.timeout(Duration::from_millis(100), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
