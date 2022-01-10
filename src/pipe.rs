/// Utility function for plugging callbags together in chain.
///
/// This utility actually doesn't rely on Callbag specifics, and is really similar to
/// [Ramda's `pipe`][ramda-pipe] or [lodash's `flow`][lodash-flow].
///
/// This exists to play nicely with the ecosystem, and to facilitate the import of the function.
///
/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/readme.js#L110-L114>
///
/// # Examples
///
/// Create a source with `pipe!`, then pass it to a [`for_each`]:
///
/// ```no_run
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{combine, for_each, interval, map, pipe, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = pipe!(
///     combine!(
///         interval(Duration::from_millis(100), nursery.clone()),
///         interval(Duration::from_millis(350), nursery.clone()),
///     ),
///     map(|(x, y)| format!("X{},Y{}", x, y)),
///     take(10),
/// );
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x: String| {
///         println!("{:?}", x);
///         actual.push(x.clone());
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(1_100), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual.len() {
///             v.push(actual.pop().ok_or("unexpected empty actual")?);
///         }
///         v
///     }[..],
///     [
///         "X2,Y0",
///         "X3,Y0",
///         "X4,Y0",
///         "X5,Y0",
///         "X6,Y0",
///         "X6,Y1",
///         "X7,Y1",
///         "X8,Y1",
///         "X9,Y1",
///         "X9,Y2",
///     ]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Or use `pipe!` to go all the way from source to sink:
///
/// ```no_run
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{combine, for_each, interval, map, pipe, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = pipe!(
///     combine!(
///         interval(Duration::from_millis(100), nursery.clone()),
///         interval(Duration::from_millis(350), nursery.clone()),
///     ),
///     map(|(x, y)| format!("X{},Y{}", x, y)),
///     take(10),
///     for_each({
///         let actual = Arc::clone(&actual);
///         move |x: String| {
///             println!("{:?}", x);
///             actual.push(x.clone());
///         }
///     }),
/// );
///
/// let nursery_out = nursery.timeout(Duration::from_millis(1_100), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual.len() {
///             v.push(actual.pop().ok_or("unexpected empty actual")?);
///         }
///         v
///     }[..],
///     [
///         "X2,Y0",
///         "X3,Y0",
///         "X4,Y0",
///         "X5,Y0",
///         "X6,Y0",
///         "X6,Y1",
///         "X7,Y1",
///         "X8,Y1",
///         "X9,Y1",
///         "X9,Y2",
///     ]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Nesting
///
/// To use `pipe!` inside another `pipe!`, you need to give the inner `pipe!` an argument, e.g.
/// `|s| pipe!(s, ...`:
///
/// ```no_run
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{combine, for_each, interval, map, pipe, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = pipe!(
///     combine!(
///         interval(Duration::from_millis(100), nursery.clone()),
///         interval(Duration::from_millis(350), nursery.clone()),
///     ),
///     |s| pipe!(
///         s,
///         map(|(x, y)| format!("X{},Y{}", x, y)),
///         take(10),
///     ),
///     for_each({
///         let actual = Arc::clone(&actual);
///         move |x: String| {
///             println!("{:?}", x);
///             actual.push(x.clone());
///         }
///     }),
/// );
///
/// let nursery_out = nursery.timeout(Duration::from_millis(1_100), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual.len() {
///             v.push(actual.pop().ok_or("unexpected empty actual")?);
///         }
///         v
///     }[..],
///     [
///         "X2,Y0",
///         "X3,Y0",
///         "X4,Y0",
///         "X5,Y0",
///         "X6,Y0",
///         "X6,Y1",
///         "X7,Y1",
///         "X8,Y1",
///         "X9,Y1",
///         "X9,Y2",
///     ]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// This means you can use `pipe!` to create a new operator:
///
/// ```no_run
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{combine, for_each, interval, map, pipe, take};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let map_then_take = |f, amount| {
///     move |s| pipe!(s, map(f), take(amount))
/// };
///
/// let source = pipe!(
///     combine!(
///         interval(Duration::from_millis(100), nursery.clone()),
///         interval(Duration::from_millis(350), nursery.clone()),
///     ),
///     |s| pipe!(
///         s,
///         map_then_take(|(x, y)| format!("X{},Y{}", x, y), 10),
///     ),
///     for_each({
///         let actual = Arc::clone(&actual);
///         move |x: String| {
///             println!("{:?}", x);
///             actual.push(x.clone());
///         }
///     }),
/// );
///
/// let nursery_out = nursery.timeout(Duration::from_millis(1_100), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual.len() {
///             v.push(actual.pop().ok_or("unexpected empty actual")?);
///         }
///         v
///     }[..],
///     [
///         "X2,Y0",
///         "X3,Y0",
///         "X4,Y0",
///         "X5,Y0",
///         "X6,Y0",
///         "X6,Y1",
///         "X7,Y1",
///         "X8,Y1",
///         "X9,Y1",
///         "X9,Y2",
///     ]
/// );
/// #
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [`for_each`]: crate::for_each()
/// [lodash-flow]: https://lodash.com/docs/#flow
/// [ramda-pipe]: https://ramdajs.com/docs/#pipe
#[macro_export]
macro_rules! pipe {
    ($a:expr, $b:expr $(,)?) => { $b($a) };
    ($a:expr, $b:expr, $($rest:expr),* $(,)?) => {
        $crate::pipe!($b($a), $($rest),*)
    };
}
