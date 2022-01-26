use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;
use never::Never;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};
use tracing::{info, Span};

use crate::{
    common::{array_queue, VariantName},
    utils::{call, tracing::instrument},
};

use callbag::{from_iter, Message};

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

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L4-L34>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_sends_array_items_iterable_to_a_puller_sink() {
    let test_fn_span = Span::current();

    let source = from_iter([10, 20, 30]);

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![10, 20, 30]);

    let talkback = ArcSwapOption::from(None);
    call!(
        source,
        Message::Handshake(Arc::new(
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
        )),
        "to source: {message:?}"
    );
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L36-L66>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_sends_array_entries_iterator_to_a_puller_sink() {
    let test_fn_span = Span::current();

    let source = from_iter(["a", "b", "c"].into_iter().enumerate());

    let downwards_expected_types = Arc::new(array_queue![
        "Handshake",
        "Data",
        "Data",
        "Data",
        "Terminate",
    ]);
    let downwards_expected = Arc::new(array_queue![(0, "a"), (1, "b"), (2, "c")]);

    let talkback = ArcSwapOption::from(None);
    call!(
        source,
        Message::Handshake(Arc::new(
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
                        assert_eq!(data, e, "downwards data is expected: {e:?}");
                    }
                    let talkback = talkback.load();
                    let talkback = talkback.as_ref().unwrap();
                    call!(talkback, Message::Pull, "to source: {message:?}");
                }
            })
            .into(),
        )),
        "to source: {message:?}"
    );
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L68-L97>
#[tracing::instrument]
// #[test_log::test] // don't blow up stdout
#[test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_does_not_blow_up_the_stack_when_iterating_something_huge() {
    let test_fn_span = Span::current();

    #[derive(Clone, Debug)]
    struct Gen {
        i: Arc<AtomicUsize>,
    }

    impl Gen {
        fn new(i: Arc<AtomicUsize>) -> Self {
            Gen { i }
        }
    }

    impl Iterator for Gen {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            let i = self.i.load(AtomicOrdering::Acquire);
            if i < 1_000_000 {
                self.i.fetch_add(1, AtomicOrdering::AcqRel);
                Some(i)
            } else {
                None
            }
        }
    }

    let i = Arc::new(AtomicUsize::new(0));
    let gen = Gen::new(Arc::clone(&i));
    let source = from_iter(gen);

    let talkback = ArcSwapOption::from(None);
    let iterated = Arc::new(AtomicBool::new(false));
    call!(
        source,
        Message::Handshake(Arc::new(
            {
                let iterated = Arc::clone(&iterated);
                move |message| {
                    instrument!(parent: &test_fn_span, "sink");
                    info!("from source: {message:?}");
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Pull, "to source: {message:?}");
                    } else if let Message::Data(_data) = message {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Pull, "to source: {message:?}");
                    } else if let Message::Error(_) | Message::Terminate = message {
                        assert_eq!(
                            i.load(AtomicOrdering::Acquire),
                            1_000_000,
                            "1 million items were iterated"
                        );
                        iterated.store(true, AtomicOrdering::Release);
                    }
                }
            }
            .into(),
        )),
        "to source: {message:?}"
    );
    assert!(
        iterated.load(AtomicOrdering::Acquire),
        "iteration happened synchronously"
    );
}

/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/test.js#L99-L129>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_stops_sending_after_source_completion() {
    let test_fn_span = Span::current();

    let source = from_iter([10, 20, 30]);

    let actual = Arc::new(SegQueue::new());
    let downwards_expected_types = Arc::new(array_queue!["Handshake", "Data"]);

    let talkback = ArcSwapOption::from(None);
    call!(
        source,
        Message::Handshake(Arc::new(
            {
                let actual = Arc::clone(&actual);
                move |message: Message<_, Never>| {
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
                        actual.push(data);
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Terminate, "to source: {message:?}");
                        call!(talkback, Message::Pull, "to source: {message:?}");
                        call!(talkback, Message::Pull, "to source: {message:?}");
                    }
                }
            }
            .into(),
        )),
        "to source: {message:?}"
    );

    assert_eq!(
        &{
            let mut v = vec![];
            while let Some(x) = actual.pop() {
                v.push(x);
            }
            v
        }[..],
        [10]
    );
}
