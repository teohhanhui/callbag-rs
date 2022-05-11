#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use crate::{
    common::array_queue,
    utils::{call, tracing::instrument},
};
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use callbag::{interval, Message};
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    arc_swap::ArcSwapOption,
    async_executors::Timer,
    async_nursery::{NurseExt, Nursery},
    std::{sync::Arc, time::Duration},
    tracing::{info, info_span, Span},
    tracing_futures::Instrument,
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
use wasm_bindgen_test::wasm_bindgen_test_configure;

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
pub mod common;
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
pub mod utils;

#[cfg(all(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    feature = "browser",
))]
wasm_bindgen_test_configure!(run_in_browser);

/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/test.js#L4-L25>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
async fn interval_50_sends_5_times_then_we_dispose_it() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let expected = Arc::new(array_queue![0, 1, 2, 3, 4]);

    let observe = {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message| {
                instrument!(parent: &test_fn_span, "sink");
                info!("from source: {message:?}");
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    assert_eq!(data, expected.pop().unwrap(), "interval sent data");
                    if expected.is_empty() {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        call!(talkback, Message::Terminate, "to source: {message:?}");
                    }
                }
            })
            .into(),
        )
    };

    call!(
        interval(Duration::from_millis(50), nursery.clone()),
        Message::Handshake(observe),
        "to source: {message:?}"
    );

    drop(nursery);
    nursery_out.await;
}

/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/test.js#L27-L47>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[tracing::instrument]
#[test_log::test(async_std::test)]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
async fn interval_1000_can_be_disposed_before_anything_is_sent() {
    let test_fn_span = Span::current();
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let observe = {
        let talkback = Arc::new(ArcSwapOption::from(None));
        Arc::new(
            {
                let nursery = nursery.clone();
                move |message| {
                    instrument!(parent: &test_fn_span, "sink", sink_span);
                    info!("from source: {message:?}");
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        {
                            let nursery = nursery
                                .clone()
                                .instrument(info_span!(parent: &sink_span, "async_task"));
                            nursery
                                .nurse({
                                    let nursery = nursery.clone();
                                    let talkback = Arc::clone(&talkback);
                                    const DURATION: Duration = Duration::from_millis(200);
                                    async move {
                                        nursery.sleep(DURATION).await;
                                        let talkback = talkback.load();
                                        let talkback = talkback.as_ref().unwrap();
                                        call!(
                                            talkback,
                                            Message::Terminate,
                                            "to source: {message:?}"
                                        );
                                    }
                                })
                                .unwrap();
                        }
                    } else if let Message::Data(_data) = message {
                        panic!("data should not be sent");
                    }
                }
            }
            .into(),
        )
    };

    call!(
        interval(Duration::from_millis(1_000), nursery.clone()),
        Message::Handshake(observe),
        "to source: {message:?}"
    );

    drop(nursery);
    nursery_out.await;
}
