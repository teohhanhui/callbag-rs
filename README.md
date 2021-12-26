#   callbag-rs

Basic [callbag][callbag-spec] factories and operators to get started with.

**Highlights:**

- Supports reactive stream programming
- Supports iterable programming (also!)
- Same operator works for both of the above
- Extensible

Imagine a hybrid between an [Observable][tc39-observable] and an [(Async)Iterable][tc39-async-iteration], that's what
callbags are all about. It's all done with a few simple callbacks, following the [callbag spec][callbag-spec].

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MIT OR Apache-2.0 licensed][license-badge]][license-url]

##  Examples

### Reactive programming examples

Pick the first 5 odd numbers from a clock that ticks every second, then start observing them:

```rust
use arc_swap::ArcSwap;
use async_nursery::Nursery;
use std::{sync::Arc, time::Duration};

use callbag::{filter, for_each, interval, map, pipe, take};

let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);

let vec = Arc::new(ArcSwap::from_pointee(vec![]));

pipe!(
    interval(Duration::from_millis(1_000), nursery.clone()),
    map(|x| x + 1),
    filter(|x| x % 2 == 1),
    take(5),
    for_each({
        let vec = Arc::clone(&vec);
        move |x| {
            println!("{}", x);
            vec.rcu(move |vec| {
                let mut vec = (**vec).clone();
                vec.push(x);
                vec
            });
        }
    }),
);

drop(nursery);
async_std::task::block_on(nursery_out);

assert_eq!(vec.load()[..], [1, 3, 5, 7, 9]);
```

### Iterable programming examples

From a range of numbers, pick 5 of them and divide them by 4, then start pulling those one by one:

```rust
use arc_swap::ArcSwap;
use std::sync::Arc;

use callbag::{for_each, from_iter, map, pipe, take};

#[derive(Clone)]
struct Range {
    i: usize,
    to: usize,
}

impl Range {
    fn new(from: usize, to: usize) -> Self {
        Range { i: from, to }
    }
}

impl Iterator for Range {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i <= self.to {
            self.i += 1;
            Some(i)
        } else {
            None
        }
    }
}

let vec = Arc::new(ArcSwap::from_pointee(vec![]));

pipe!(
    from_iter(Range::new(40, 99)), // 10, 10.25, 10.5, 10.75, 11
    take(5), // 40, 41, 42, 43, 44
    map(|x| x as f64 / 4.0), // 10, 10.25, 10.5, 10.75, 11
    for_each({
        let vec = Arc::clone(&vec);
        move |x| {
            println!("{}", x);
            vec.rcu(move |vec| {
                let mut vec = (**vec).clone();
                vec.push(x);
                vec
            });
        }
    }),
);

assert_eq!(vec.load()[..], [10.0, 10.25, 10.5, 10.75, 11.0]);
```

##  API

The list below shows what's included.

### Source factories

- [from_iter](https://docs.rs/callbag/latest/callbag/fn.from_iter.html)
- [interval](https://docs.rs/callbag/latest/callbag/fn.interval.html)

### Sink factories

- [for_each](https://docs.rs/callbag/latest/callbag/fn.for_each.html)

### Transformation operators

- [map](https://docs.rs/callbag/latest/callbag/fn.map.html)
- [scan](https://docs.rs/callbag/latest/callbag/fn.scan.html)
- [flatten](https://docs.rs/callbag/latest/callbag/fn.flatten.html)

### Filtering operators

- [take](https://docs.rs/callbag/latest/callbag/fn.take.html)
- [skip](https://docs.rs/callbag/latest/callbag/fn.skip.html)
- [filter](https://docs.rs/callbag/latest/callbag/fn.filter.html)

### Combination operators

- [merge!](https://docs.rs/callbag/latest/callbag/macro.merge.html)
- [concat!](https://docs.rs/callbag/latest/callbag/macro.concat.html)
- [combine!](https://docs.rs/callbag/latest/callbag/macro.combine.html)

### Utilities

- [share](https://docs.rs/callbag/latest/callbag/fn.share.html)
- [pipe!](https://docs.rs/callbag/latest/callbag/macro.pipe.html)

##  Terminology

- **source**: a callbag that delivers data
- **sink**: a callbag that receives data
- **puller sink**: a sink that actively requests data from the source
- **pullable source**: a source that delivers data only on demand (on receiving a request)
- **listener sink**: a sink that passively receives data from the source
- **listenable source**: source which sends data to the sink without waiting for requests
- **operator**: a callbag based on another callbag which applies some operation

##  License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[callbag-spec]: https://github.com/callbag/callbag
[crates-badge]: https://img.shields.io/crates/v/callbag
[crates-url]: https://crates.io/crates/callbag
[docs-badge]: https://img.shields.io/docsrs/callbag
[docs-url]: https://docs.rs/callbag
[license-badge]: https://img.shields.io/crates/l/callbag
[license-url]: https://github.com/teohhanhui/callbag-rs#license
[tc39-async-iteration]: https://github.com/tc39/proposal-async-iteration
[tc39-observable]: https://github.com/tc39/proposal-observable
