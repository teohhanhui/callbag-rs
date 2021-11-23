use std::{
    iter::IntoIterator,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

use crate::{Message, Source};

/// Converts an iterator to a callbag pullable source.
///
/// It only sends data when requested.
///
/// See <https://github.com/staltz/callbag-from-iter/blob/a5942d3a23da500b771d2078f296df2e41235b3a/index.js#L1-L34>
pub fn from_iter<T: 'static, I: 'static>(iter: I) -> Source<T>
where
    T: Send + Sync,
    I: IntoIterator<Item = T> + Send + Sync + Clone,
    <I as IntoIterator>::IntoIter: Send + Sync,
{
    (move |message| {
        if let Message::Handshake(sink) = message {
            let iter = Arc::new(RwLock::new(iter.clone().into_iter()));
            let sink = Arc::new(sink);
            let in_loop = Arc::new(AtomicBool::new(false));
            let got_pull = Arc::new(AtomicBool::new(false));
            let completed = Arc::new(AtomicBool::new(false));
            let res = Arc::new(RwLock::new(None));
            let res_done = Arc::new(AtomicBool::new(false));
            let r#loop = {
                let sink = sink.clone();
                let in_loop = in_loop.clone();
                let got_pull = got_pull.clone();
                let completed = completed.clone();
                let res_done = res_done.clone();
                move || {
                    in_loop.store(true, AtomicOrdering::Release);
                    while got_pull.load(AtomicOrdering::Acquire)
                        && !completed.load(AtomicOrdering::Acquire)
                    {
                        got_pull.store(false, AtomicOrdering::Release);
                        {
                            let iter = &mut *iter.write().unwrap();
                            let mut res = res.write().unwrap();
                            *res = iter.next();
                            res_done.store(res.is_none(), AtomicOrdering::Release);
                        }
                        if res_done.load(AtomicOrdering::Acquire) {
                            sink(Message::Terminate);
                            break;
                        } else {
                            let res = {
                                let res = &mut *res.write().unwrap();
                                res.take().unwrap()
                            };
                            sink(Message::Data(res));
                        }
                    }
                    in_loop.store(false, AtomicOrdering::Release);
                }
            };
            sink(Message::Handshake(
                (move |message| {
                    if completed.load(AtomicOrdering::Acquire) {
                        return;
                    }

                    match message {
                        Message::Handshake(_) => {
                            panic!("sink handshake has already occurred");
                        }
                        Message::Data(_) => {
                            panic!("sink must not send data");
                        }
                        Message::Pull => {
                            got_pull.store(true, AtomicOrdering::Release);
                            if !in_loop.load(AtomicOrdering::Acquire)
                                && !res_done.load(AtomicOrdering::Acquire)
                            {
                                r#loop();
                            }
                        }
                        Message::Error(_) | Message::Terminate => {
                            completed.store(true, AtomicOrdering::Release);
                        }
                    }
                })
                .into(),
            ));
        }
    })
    .into()
}
