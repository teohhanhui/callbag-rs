use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{Message, Source};

/// Callbag sink that consumes both pullable and listenable sources.
///
/// When called on a pullable source, it will iterate through its data. When called on a listenable
/// source, it will observe its data.
///
/// See <https://github.com/staltz/callbag-for-each/blob/a7550690afca2a27324ea5634a32a313f826d61a/readme.js#L40-L47>
///
/// # Examples
///
/// Consume a pullable source:
///
/// ```
/// use arc_swap::ArcSwap;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter};
///
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = from_iter([10, 20, 30, 40]);
///
/// for_each({
///     let vec = Arc::clone(&vec);
///     move |x| {
///         println!("{}", x);
///         vec.rcu(move |vec| {
///             let mut vec = (**vec).clone();
///             vec.push(x);
///             vec
///         });
///     }
/// })(source);
///
/// assert_eq!(vec.load()[..], [10, 20, 30, 40]);
/// ```
///
/// Consume a listenable source:
///
/// ```
/// use arc_swap::ArcSwap;
/// use async_nursery::Nursery;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{for_each, interval};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = interval(Duration::from_millis(1_000), nursery.clone());
///
/// for_each({
///     let vec = Arc::clone(&vec);
///     move |x| {
///         println!("{}", x);
///         vec.rcu(move |vec| {
///             let mut vec = (**vec).clone();
///             vec.push(x);
///             vec
///         });
///     }
/// })(source);
///
/// drop(nursery);
/// async_std::task::block_on(async_std::future::timeout(
///     Duration::from_millis(4_500),
///     nursery_out,
/// ));
///
/// assert_eq!(vec.load()[..], [0, 1, 2, 3]);
/// ```
pub fn for_each<T: 'static, F: 'static, S>(f: F) -> Box<dyn Fn(S)>
where
    F: Fn(T) + Send + Sync + Clone,
    S: Into<Arc<Source<T>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<T>> = source.into();
        let talkback = ArcSwapOption::from(None);
        source(Message::Handshake(Arc::new(
            {
                let f = f.clone();
                move |message| match message {
                    Message::Handshake(source) => {
                        talkback.store(Some(source));
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    }
                    Message::Data(data) => {
                        f(data);
                        let talkback = talkback.load();
                        let talkback = talkback.as_ref().unwrap();
                        talkback(Message::Pull);
                    }
                    Message::Pull => {
                        panic!("source must not pull");
                    }
                    Message::Error(_) => {}
                    Message::Terminate => {}
                }
            }
            .into(),
        )));
    })
}
