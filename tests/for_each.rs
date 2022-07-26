use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc, RwLock,
};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{for_each, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    never::Never,
    std::time::Duration,
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

/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/test.js#L4-L50>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_iterates_a_finite_pullable_source() {
    let test_fn_span = Span::current();

    let upwards_expected = Arc::new(array_queue!["Pull", "Pull", "Pull"]);
    let downwards_expected = Arc::new(array_queue!["a", "b", "c"]);

    let sink = for_each({
        let test_fn_span = test_fn_span.clone();
        move |x| {
            instrument!(parent: &test_fn_span, "sink");
            info!("from source: {x}");
            assert_eq!(
                x,
                downwards_expected.pop().unwrap(),
                "downwards data is expected"
            );
        }
    });

    let make_source = move || {
        let sent = Arc::new(AtomicUsize::new(0));
        let sink_ref = Arc::new(ArcSwapOption::from(None));
        let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source = Arc::new(
            {
                let source_ref = Arc::clone(&source_ref);
                move |message| {
                    instrument!(parent: &test_fn_span, "source", source_span);
                    info!("from sink: {message:?}");
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
                    assert!(!upwards_expected.is_empty(), "source can be pulled");
                    {
                        let expected = upwards_expected.pop().unwrap();
                        assert_eq!(message.variant_name(), expected, "upwards type is expected");
                    }
                    if sent.load(AtomicOrdering::Acquire) == 0 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        call!(sink_ref, Message::Data("a"), "to sink: {message:?}");
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 1 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        call!(sink_ref, Message::Data("b"), "to sink: {message:?}");
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 2 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        call!(sink_ref, Message::Data("c"), "to sink: {message:?}");
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

    let source = make_source();
    sink(source);
}

/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/test.js#L52-L109>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
async fn it_observes_an_async_finite_listenable_source() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected = Arc::new(array_queue!["Handshake", "Pull", "Pull", "Pull", "Pull"]);
    let downwards_expected = Arc::new(array_queue![10, 20, 30]);

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

    let source = make_source();
    for_each(move |x| {
        instrument!(parent: &test_fn_span, "sink");
        info!("from source: {x}");
        let e = downwards_expected.pop().unwrap();
        assert_eq!(x, e, "downwards data is expected: {e}");
    })(source);

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
