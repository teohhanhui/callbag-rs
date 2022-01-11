use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc, RwLock,
};
use tracing::info;

use crate::common::MessagePredicate;

use callbag::{for_each, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_executors::{Timer, TimerExt},
    async_nursery::{NurseExt, Nursery},
    std::time::Duration,
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

/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/test.js#L4-L50>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_iterates_a_finite_pullable_source() {
    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected = ["a", "b", "c"];
    let downwards_expected = {
        let q = SegQueue::new();
        downwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let sink = for_each(move |x| {
        info!("down: {}", x);
        assert_eq!(
            x,
            downwards_expected.pop().unwrap(),
            "downwards data is expected"
        );
    });

    let make_source = move || {
        let sent = Arc::new(AtomicUsize::new(0));
        let sink_ref = Arc::new(ArcSwapOption::from(None));
        let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
        let source = Arc::new(
            {
                let source_ref = Arc::clone(&source_ref);
                move |message| {
                    info!("up: {:?}", message);
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
                    }
                    if sent.load(AtomicOrdering::Acquire) == 3 {
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Terminate);
                        return;
                    }
                    assert!(!upwards_expected.is_empty(), "source can be pulled");
                    {
                        let expected = upwards_expected.pop().unwrap();
                        assert!(expected.0(&message), "upwards type is expected");
                    }
                    if sent.load(AtomicOrdering::Acquire) == 0 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data("a"));
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 1 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data("b"));
                        return;
                    }
                    if sent.load(AtomicOrdering::Acquire) == 2 {
                        sent.fetch_add(1, AtomicOrdering::AcqRel);
                        let sink_ref = sink_ref.load();
                        let sink_ref = sink_ref.as_ref().unwrap();
                        sink_ref(Message::Data("c"));
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
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_observes_an_async_finite_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
    let nursery = nursery.in_current_span();

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected = {
        let q = SegQueue::new();
        upwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };
    let downwards_expected = [10, 20, 30];
    let downwards_expected = {
        let q = SegQueue::new();
        downwards_expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        info!("up: {:?}", message);
                        {
                            let e = upwards_expected.pop().unwrap();
                            assert!(e.0(&message), "upwards type is expected: {}", e.1);
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
                                            if sent.load(AtomicOrdering::Acquire) == 0 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(10));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 1 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(20));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 2 {
                                                sent.fetch_add(1, AtomicOrdering::AcqRel);
                                                sink(Message::Data(30));
                                                continue;
                                            }
                                            if sent.load(AtomicOrdering::Acquire) == 3 {
                                                sink(Message::Terminate);
                                                break;
                                            }
                                        }
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

    let source = make_source();
    for_each(move |x| {
        info!("down: {}", x);
        let e = downwards_expected.pop().unwrap();
        assert_eq!(x, e, "downwards data is expected: {}", e);
    })(source);

    let nursery_out = nursery.timeout(Duration::from_millis(700), nursery_out);
    drop(nursery);
    nursery_out.await.ok();
}
