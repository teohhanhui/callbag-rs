use arc_swap::ArcSwapOption;
use std::sync::Arc;

use crate::{Message, Source};

/// Callbag operator that conditionally lets data pass through.
///
/// Works on either pullable or listenable sources.
///
/// See <https://github.com/staltz/callbag-filter/blob/01212b2d17622cae31545200235e9db3f1b0e235/readme.js#L23-L36>
///
/// # Examples
///
/// ```
/// use crossbeam_queue::SegQueue;
/// use std::sync::Arc;
///
/// use callbag::{filter, for_each, from_iter};
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = filter(|x| x % 2 == 1)(from_iter([1, 2, 3, 4, 5]));
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{}", x);
///         actual.push(x);
///     }
/// })(source);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         for _i in 0..actual.len() {
///             v.push(actual.pop().unwrap());
///         }
///         v
///     }[..],
///     [1, 3, 5]
/// );
/// ```
pub fn filter<I: 'static, F: 'static, S>(condition: F) -> Box<dyn Fn(S) -> Source<I>>
where
    F: Fn(&I) -> bool + Send + Sync + Clone,
    S: Into<Arc<Source<I>>>,
{
    Box::new(move |source| {
        let source: Arc<Source<I>> = source.into();
        {
            let condition = condition.clone();
            move |message| {
                if let Message::Handshake(sink) = message {
                    let talkback = Arc::new(ArcSwapOption::from(None));
                    source(Message::Handshake(Arc::new(
                        {
                            let condition = condition.clone();
                            move |message| {
                                let talkback = Arc::clone(&talkback);
                                match message {
                                    Message::Handshake(source) => {
                                        talkback.store(Some(source));
                                        sink(Message::Handshake(Arc::new(
                                            (move |message| match message {
                                                Message::Handshake(_) => {
                                                    panic!("sink handshake has already occurred");
                                                },
                                                Message::Data(_) => {
                                                    panic!("sink must not send data");
                                                },
                                                Message::Pull => {
                                                    let talkback = talkback.load();
                                                    let source = talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    source(Message::Pull);
                                                },
                                                Message::Error(error) => {
                                                    let talkback = talkback.load();
                                                    let source = talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    source(Message::Error(error));
                                                },
                                                Message::Terminate => {
                                                    let talkback = talkback.load();
                                                    let source = talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    source(Message::Terminate);
                                                },
                                            })
                                            .into(),
                                        )));
                                    },
                                    Message::Data(data) => {
                                        if condition(&data) {
                                            sink(Message::Data(data));
                                        } else {
                                            let talkback = talkback.load();
                                            let talkback =
                                                talkback.as_ref().expect("source talkback not set");
                                            talkback(Message::Pull);
                                        }
                                    },
                                    Message::Pull => {
                                        panic!("source must not pull");
                                    },
                                    Message::Error(error) => {
                                        sink(Message::Error(error));
                                    },
                                    Message::Terminate => {
                                        sink(Message::Terminate);
                                    },
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
