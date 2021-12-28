use std::sync::Arc;

use crate::{Message, Source};

/// Callbag operator that applies a transformation on data passing through it.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-map/blob/b9d984b78bf4301d0525b21f928d896842e17a0a/readme.js#L24-L29>
///
/// # Examples
///
/// ```
/// use arc_swap::ArcSwap;
/// use std::sync::Arc;
///
/// use callbag::{for_each, from_iter, map};
///
/// let vec = Arc::new(ArcSwap::from_pointee(vec![]));
///
/// let source = map(|x| (x as f64 * 0.1) as usize)(from_iter([10, 20, 30, 40]));
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
/// assert_eq!(vec.load()[..], [1, 2, 3, 4]);
/// ```
pub fn map<I: 'static, O: 'static, F: 'static, S>(f: F) -> Box<dyn Fn(S) -> Source<O>>
where
    F: Fn(I) -> O + Send + Sync + Clone,
    S: Into<Arc<Source<I>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<I>> = source.into();
        {
            let f = f.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    source(Message::Handshake(Arc::new(
                        {
                            let f = f.clone();
                            move |message| match message {
                                Message::Handshake(source) => {
                                    sink(Message::Handshake(Arc::new(
                                        (move |message| match message {
                                            Message::Handshake(_) => {
                                                panic!("sink handshake has already occurred");
                                            }
                                            Message::Data(_) => {
                                                panic!("sink must not send data");
                                            }
                                            Message::Pull => {
                                                source(Message::Pull);
                                            }
                                            Message::Error(error) => {
                                                source(Message::Error(error));
                                            }
                                            Message::Terminate => {
                                                source(Message::Terminate);
                                            }
                                        })
                                        .into(),
                                    )));
                                }
                                Message::Data(data) => {
                                    sink(Message::Data(f(data)));
                                }
                                Message::Pull => {
                                    panic!("source must not pull");
                                }
                                Message::Error(error) => {
                                    sink(Message::Error(error));
                                }
                                Message::Terminate => {
                                    sink(Message::Terminate);
                                }
                            }
                        }
                        .into(),
                    )));
                }
            }
        }
        .into()
    })
}
