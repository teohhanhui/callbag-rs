use arc_swap::ArcSwapOption;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

use crate::common::MessagePredicate;

use callbag::{for_each, Message, Source};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    async_nursery::{NurseExt, Nursery},
    futures_timer::Delay,
    std::{pin::Pin, time::Duration},
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
#[test]
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
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected = ["a", "b", "c"];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let sink = for_each(move |x| {
        println!("down: {}", x);
        assert_eq!(
            x,
            {
                let downwards_expected = &mut *downwards_expected.write().unwrap();
                downwards_expected.pop_front().unwrap()
            },
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
                    println!("up: {:?}", message);
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
                    {
                        let upwards_expected = &mut *upwards_expected.write().unwrap();
                        assert!(!upwards_expected.is_empty(), "source can be pulled");
                        let expected = upwards_expected.pop_front().unwrap();
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
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn it_observes_an_async_finite_listenable_source() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let upwards_expected: Vec<(MessagePredicate<_, _>, &str)> = vec![
        (|m| matches!(m, Message::Handshake(_)), "Message::Handshake"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
        (|m| matches!(m, Message::Pull), "Message::Pull"),
    ];
    let upwards_expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(upwards_expected.into()));
    let downwards_expected = [10, 20, 30];
    let downwards_expected: Arc<RwLock<VecDeque<_>>> =
        Arc::new(RwLock::new(downwards_expected.into()));

    let make_source = {
        let nursery = nursery.clone();
        move || {
            let sent = Arc::new(AtomicUsize::new(0));
            let source_ref: Arc<RwLock<Option<Arc<Source<_>>>>> = Arc::new(RwLock::new(None));
            let source = Arc::new(
                {
                    let source_ref = Arc::clone(&source_ref);
                    move |message| {
                        println!("up: {:?}", message);
                        {
                            let upwards_expected = &mut *upwards_expected.write().unwrap();
                            let e = upwards_expected.pop_front().unwrap();
                            assert!(e.0(&message), "upwards type is expected: {}", e.1);
                        }
                        if let Message::Handshake(sink) = message {
                            nursery
                                .clone()
                                .nurse({
                                    let sent = Arc::clone(&sent);
                                    let sink = Arc::clone(&sink);
                                    const DURATION: Duration = Duration::from_millis(100);
                                    let mut interval = Delay::new(DURATION);
                                    async move {
                                        loop {
                                            Pin::new(&mut interval).await;
                                            interval.reset(DURATION);
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
        println!("down: {}", x);
        let downwards_expected = &mut *downwards_expected.write().unwrap();
        let e = downwards_expected.pop_front().unwrap();
        assert_eq!(x, e, "downwards data is expected: {}", e);
    })(source);

    drop(nursery);
    async_std::future::timeout(Duration::from_millis(700), nursery_out)
        .await
        .ok();
}
