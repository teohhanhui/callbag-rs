#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use callbag::{interval, Message};
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
use {
    arc_swap::ArcSwapOption,
    async_nursery::{NurseExt, Nursery},
    futures_timer::Delay,
    std::{
        collections::VecDeque,
        sync::{Arc, RwLock},
        time::Duration,
    },
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

/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/test.js#L4-L25>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn interval_50_sends_5_times_then_we_dispose_it() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let expected = [0, 1, 2, 3, 4];
    let expected: Arc<RwLock<VecDeque<_>>> = Arc::new(RwLock::new(expected.into()));

    let observe = {
        let talkback = ArcSwapOption::from(None);
        Arc::new(
            (move |message| {
                println!("down: {:?}", message);
                if let Message::Handshake(source) = message {
                    talkback.store(Some(source));
                } else if let Message::Data(data) = message {
                    assert_eq!(
                        data,
                        {
                            let expected = &mut *expected.write().unwrap();
                            expected.pop_front().unwrap()
                        },
                        "interval sent data"
                    );
                    let expected = &*expected.read().unwrap();
                    if expected.is_empty() {
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Terminate);
                    }
                }
            })
            .into(),
        )
    };

    interval(Duration::from_millis(50), nursery.clone())(Message::Handshake(observe));

    drop(nursery);
    nursery_out.await;
}

/// See <https://github.com/staltz/callbag-interval/blob/45d4fd8fd977bdf2babb27f67e740b0ff0b44e1e/test.js#L27-L47>
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi")))]
#[async_std::test]
#[cfg_attr(
    all(
        all(target_arch = "wasm32", not(target_os = "wasi")),
        feature = "browser",
    ),
    wasm_bindgen_test
)]
async fn interval_1000_can_be_disposed_before_anything_is_sent() {
    let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

    let observe = {
        let talkback = Arc::new(ArcSwapOption::from(None));
        Arc::new(
            {
                let nursery = nursery.clone();
                move |message| {
                    println!("down: {:?}", message);
                    if let Message::Handshake(source) = message {
                        talkback.store(Some(source));
                        nursery
                            .clone()
                            .nurse({
                                let talkback = Arc::clone(&talkback);
                                let timeout = Delay::new(Duration::from_millis(200));
                                async move {
                                    timeout.await;
                                    let talkback = talkback.load();
                                    let talkback = talkback.as_ref().unwrap();
                                    talkback(Message::Terminate);
                                }
                            })
                            .unwrap();
                    } else if let Message::Data(_data) = message {
                        panic!("data should not be sent");
                    }
                }
            }
            .into(),
        )
    };

    interval(Duration::from_millis(1000), nursery.clone())(Message::Handshake(observe));

    drop(nursery);
    nursery_out.await;
}
