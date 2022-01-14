use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{Message, Source};

/// Callbag operator that flattens a higher-order callbag source.
///
/// Like [ReactiveX `switch`][reactivex-switch] or [xstream `flatten`][xstream-flatten]. Use it
/// with [`map`] to get behavior equivalent to [RxJS `switchMap`][rxjs-switch-map].
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-flatten/blob/9d08c8807802243517697dd7401a9d5d2ba69c24/index.js#L1-L43>
///
/// # Examples
///
/// ## Listenables
///
/// ## Pullables
///
/// Loop over two iterables (such as arrays) and combine their values together:
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{flatten, for_each, from_iter, map, pipe};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = pipe!(
///     from_iter("hi".chars()),
///     map(|r#char| pipe!(
///         from_iter([10, 20, 30]),
///         map(move |num| format!("{}{}", r#char, num)),
///     )),
///     flatten,
///     for_each({
///         let actual = Arc::clone(&actual);
///         move |x: String| {
///             println!("{}", x);
///             actual.push(x.clone());
///         }
///     }),
/// );
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     ["h10", "h20", "h30", "i10", "i20", "i30"]
/// );
/// ```
///
/// [`map`]: crate::map()
/// [reactivex-switch]: https://reactivex.io/documentation/operators/switch.html
/// [rxjs-switch-map]: https://rxjs.dev/api/operators/switchMap
/// [xstream-flatten]: https://github.com/staltz/xstream#flatten
pub fn flatten<T: 'static, S, R: 'static>(source: S) -> Source<T>
where
    S: Into<Arc<Source<R>>>,
    R: Into<Arc<Source<T>>>,
{
    let source: Arc<Source<R>> = source.into();
    (move |message| {
        if let Message::Handshake(sink) = message {
            let outer_talkback: Arc<ArcSwapOption<Source<R>>> = Arc::new(ArcSwapOption::from(None));
            let inner_talkback: Arc<ArcSwapOption<Source<T>>> = Arc::new(ArcSwapOption::from(None));
            let talkback: Arc<Source<T>> = Arc::new(
                {
                    let outer_talkback = Arc::clone(&outer_talkback);
                    let inner_talkback = Arc::clone(&inner_talkback);
                    move |message| match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        },
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        },
                        Message::Pull => {
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Pull);
                            } else if let Some(outer_talkback) = &*outer_talkback.load() {
                                outer_talkback(Message::Pull);
                            }
                        },
                        Message::Error(_) | Message::Terminate => {
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Terminate);
                            }
                            if let Some(outer_talkback) = &*outer_talkback.load() {
                                outer_talkback(Message::Terminate);
                            }
                        },
                    }
                }
                .into(),
            );
            source(Message::Handshake(Arc::new(
                (move |message| {
                    let sink = Arc::clone(&sink);
                    let outer_talkback = Arc::clone(&outer_talkback);
                    let inner_talkback = Arc::clone(&inner_talkback);
                    match message {
                        Message::Handshake(source) => {
                            outer_talkback.store(Some(source));
                            sink(Message::Handshake(Arc::clone(&talkback)));
                        },
                        Message::Data(inner_source) => {
                            let inner_source: Arc<Source<T>> = inner_source.into();
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Terminate);
                            }
                            inner_source(Message::Handshake(Arc::new(
                                (move |message| match message {
                                    Message::Handshake(source) => {
                                        inner_talkback.store(Some(source));
                                        let inner_talkback = inner_talkback.load();
                                        let inner_talkback = inner_talkback
                                            .as_ref()
                                            .expect("inner source talkback not set");
                                        inner_talkback(Message::Pull);
                                    },
                                    Message::Data(data) => {
                                        sink(Message::Data(data));
                                    },
                                    Message::Pull => {
                                        panic!("source must not pull");
                                    },
                                    Message::Error(error) => {
                                        if let Some(outer_talkback) = &*outer_talkback.load() {
                                            outer_talkback(Message::Terminate);
                                        }
                                        sink(Message::Error(error));
                                    },
                                    Message::Terminate => {
                                        if outer_talkback.load().is_none() {
                                            sink(Message::Terminate);
                                        } else {
                                            inner_talkback.store(None);
                                            let outer_talkback = outer_talkback.load();
                                            let outer_talkback = outer_talkback
                                                .as_ref()
                                                .expect("outer source talkback not set");
                                            outer_talkback(Message::Pull);
                                        }
                                    },
                                })
                                .into(),
                            )));
                        },
                        Message::Pull => {
                            panic!("source must not pull");
                        },
                        Message::Error(error) => {
                            if let Some(inner_talkback) = &*inner_talkback.load() {
                                inner_talkback(Message::Terminate);
                            }
                            sink(Message::Error(error));
                        },
                        Message::Terminate => {
                            if inner_talkback.load().is_none() {
                                sink(Message::Terminate);
                            } else {
                                outer_talkback.store(None);
                            }
                        },
                    }
                })
                .into(),
            )));
        }
    })
    .into()
}
