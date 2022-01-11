use crossbeam_queue::SegQueue;
use std::sync::Arc;

use callbag::{filter, for_each, from_iter, map, pipe};

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

/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/test.js#L8-L17>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_calls_first_order_functions_in_sequence_ltr() {
    let res = pipe!(
        2,          // 2
        |x| x * 10, // 20
        |x| x - 3,  // 17
        |x| x + 5,  // 22
    );
    assert_eq!(res, 22);
}

/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/test.js#L19-L30>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_calls_first_order_functions_in_a_nested_pipe() {
    let res = pipe!(
        2, // 2
        |s| pipe!(
            s,
            |x| x * 10, // 20
            |x| x - 3,  // 17
        ),
        |x| x + 5, // 22
    );
    assert_eq!(res, 22);
}

/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/test.js#L64-L78>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_can_be_used_with_common_callbag_utilities() {
    let expected = [1, 3];
    let expected = {
        let q = SegQueue::new();
        expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };
    pipe!(
        from_iter([10, 20, 30, 40]),
        map(|x| x / 10),
        filter(|x| x % 2 != 0),
        for_each(move |x| {
            assert_eq!(x, expected.pop().unwrap());
        }),
    );
}

/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/test.js#L80-L96>
#[tracing::instrument]
#[test_log::test]
#[cfg_attr(
    all(target_arch = "wasm32", not(target_os = "wasi")),
    wasm_bindgen_test
)]
fn it_can_be_nested_with_callbag_utilities() {
    let expected = [1, 3];
    let expected = {
        let q = SegQueue::new();
        expected.into_iter().for_each(|item| q.push(item));
        Arc::new(q)
    };
    pipe!(
        from_iter([10, 20, 30, 40]),
        |s| pipe!(s, map(|x| x / 10), filter(|x| x % 2 != 0)),
        for_each(move |x| {
            assert_eq!(x, expected.pop().unwrap());
        }),
    );
}
