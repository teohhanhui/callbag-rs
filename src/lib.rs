//! # Rust implementation of the [callbag spec][callbag-spec] for reactive/iterable programming
//!
//! Basic [callbag][callbag-spec] factories and operators to get started with.
//!
//! **Highlights:**
//!
//! - Supports reactive stream programming
//! - Supports iterable programming (also!)
//! - Same operator works for both of the above
//! - Extensible
//!
//! Imagine a hybrid between an [Observable][tc39-observable] and an
//! [(Async)Iterable][tc39-async-iteration], that's what callbags are all about. It's all done with
//! a few simple callbacks, following the [callbag spec][callbag-spec].
//!
//! # Examples
//!
//! ## Reactive programming examples
//!
//! Pick the first 5 odd numbers from a clock that ticks every second, then start observing them:
//!
//! ```
//! use arc_swap::ArcSwap;
//! use async_nursery::Nursery;
//! use std::{sync::Arc, time::Duration};
//!
//! use callbag::{filter, for_each, interval, map, pipe, take};
//!
//! let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
//!
//! let vec = Arc::new(ArcSwap::from_pointee(vec![]));
//!
//! pipe!(
//!     interval(Duration::from_millis(1_000), nursery.clone()),
//!     map(|x| x + 1),
//!     filter(|x| x % 2 == 1),
//!     take(5),
//!     for_each({
//!         let vec = Arc::clone(&vec);
//!         move |x| {
//!             println!("{}", x);
//!             vec.rcu(move |vec| {
//!                 let mut vec = (**vec).clone();
//!                 vec.push(x);
//!                 vec
//!             });
//!         }
//!     }),
//! );
//!
//! drop(nursery);
//! async_std::task::block_on(nursery_out);
//!
//! assert_eq!(vec.load()[..], [1, 3, 5, 7, 9]);
//! ```
//!
//! ## Iterable programming examples
//!
//! From a range of numbers, pick 5 of them and divide them by 4, then start pulling those one by one:
//!
//! ```
//! use arc_swap::ArcSwap;
//! use std::sync::Arc;
//!
//! use callbag::{for_each, from_iter, map, pipe, take};
//!
//! #[derive(Clone)]
//! struct Range {
//!     i: usize,
//!     to: usize,
//! }
//!
//! impl Range {
//!     fn new(from: usize, to: usize) -> Self {
//!         Range { i: from, to }
//!     }
//! }
//!
//! impl Iterator for Range {
//!     type Item = usize;
//!
//!     fn next(&mut self) -> Option<Self::Item> {
//!         let i = self.i;
//!         if i <= self.to {
//!             self.i += 1;
//!             Some(i)
//!         } else {
//!             None
//!         }
//!     }
//! }
//!
//! let vec = Arc::new(ArcSwap::from_pointee(vec![]));
//!
//! pipe!(
//!     from_iter(Range::new(40, 99)),
//!     take(5),
//!     map(|x| x as f64 / 4.0),
//!     for_each({
//!         let vec = Arc::clone(&vec);
//!         move |x| {
//!             println!("{}", x);
//!             vec.rcu(move |vec| {
//!                 let mut vec = (**vec).clone();
//!                 vec.push(x);
//!                 vec
//!             });
//!         }
//!     }),
//! );
//!
//! assert_eq!(vec.load()[..], [10.0, 10.25, 10.5, 10.75, 11.0]);
//! ```
//!
//! # API
//!
//! The list below shows what's included.
//!
//! ## Source factories
//!
//! - [from_iter][crate::from_iter()]
//! - [interval][crate::interval()]
//!
//! ## Sink factories
//!
//! - [for_each][crate::for_each()]
//!
//! ## Transformation operators
//!
//! - [map][crate::map()]
//! - [scan][crate::scan()]
//! - [flatten][crate::flatten()]
//!
//! ## Filtering operators
//!
//! - [take][crate::take()]
//! - [skip][crate::skip()]
//! - [filter][crate::filter()]
//!
//! ## Combination operators
//!
//! - [merge!][crate::merge!]
//! - [concat!][crate::concat!]
//! - [combine!][crate::combine!]
//!
//! ## Utilities
//!
//! - [share][crate::share()]
//! - [pipe!][crate::pipe!]
//!
//! # Terminology
//!
//! - **source**: a callbag that delivers data
//! - **sink**: a callbag that receives data
//! - **puller sink**: a sink that actively requests data from the source
//! - **pullable source**: a source that delivers data only on demand (on receiving a request)
//! - **listener sink**: a sink that passively receives data from the source
//! - **listenable source**: source which sends data to the sink without waiting for requests
//! - **operator**: a callbag based on another callbag which applies some operation
//!
//! [callbag-spec]: https://github.com/callbag/callbag
//! [tc39-async-iteration]: https://github.com/tc39/proposal-async-iteration
//! [tc39-observable]: https://github.com/tc39/proposal-observable

#[cfg(feature = "combine")]
pub use crate::combine::combine;
#[cfg(feature = "concat")]
pub use crate::concat::concat;
pub use crate::core::*;
#[cfg(feature = "filter")]
pub use crate::filter::filter;
#[cfg(feature = "flatten")]
pub use crate::flatten::flatten;
#[cfg(feature = "for_each")]
pub use crate::for_each::for_each;
#[cfg(feature = "from_iter")]
pub use crate::from_iter::from_iter;
#[cfg(feature = "interval")]
pub use crate::interval::interval;
#[cfg(feature = "map")]
pub use crate::map::map;
#[cfg(feature = "merge")]
pub use crate::merge::merge;
#[cfg(feature = "scan")]
pub use crate::scan::scan;
#[cfg(feature = "share")]
pub use crate::share::share;
#[cfg(feature = "skip")]
pub use crate::skip::skip;
#[cfg(feature = "take")]
pub use crate::take::take;

#[cfg(feature = "combine")]
mod combine;
#[cfg(feature = "concat")]
mod concat;
mod core;
#[cfg(feature = "filter")]
mod filter;
#[cfg(feature = "flatten")]
mod flatten;
#[cfg(feature = "for_each")]
mod for_each;
#[cfg(feature = "from_iter")]
mod from_iter;
#[cfg(feature = "interval")]
mod interval;
#[cfg(feature = "map")]
mod map;
#[cfg(feature = "merge")]
mod merge;
#[cfg(feature = "pipe")]
mod pipe;
#[cfg(feature = "scan")]
mod scan;
#[cfg(feature = "share")]
mod share;
#[cfg(feature = "skip")]
mod skip;
#[cfg(feature = "take")]
mod take;

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
