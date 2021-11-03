use std::{
    iter::IntoIterator,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, RwLock,
    },
};

use crate::{Callbag, Message};

pub fn from_iter<T: 'static, I: 'static>(iter: I) -> Callbag<T>
where
    T: Send + Sync,
    I: IntoIterator<Item = T> + Send + Sync + Clone,
    <I as std::iter::IntoIterator>::IntoIter: Send + Sync,
{
    (move |message| {
        if let Message::Start(sink) = message {
            let iter = Arc::new(RwLock::new(iter.clone().into_iter()));
            let sink = Arc::new(RwLock::new(move |message| {
                sink(message);
            }));
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
                            let mut iter = iter.write().unwrap();
                            let mut res = res.write().unwrap();
                            *res = iter.next();
                            res_done.store(res.is_none(), AtomicOrdering::Release);
                        }
                        if res_done.load(AtomicOrdering::Acquire) {
                            let sink = sink.read().unwrap();
                            sink(Message::End);
                            break;
                        } else {
                            let mut res = res.write().unwrap();
                            let sink = sink.read().unwrap();
                            sink(Message::Data(res.take().unwrap()));
                        }
                    }
                    in_loop.store(false, AtomicOrdering::Release);
                }
            };
            let sink = {
                let sink = sink.read().unwrap();
                sink
            };
            sink(Message::Start(
                ({
                    move |t| {
                        if completed.load(AtomicOrdering::Acquire) {
                            return;
                        }

                        if let Message::Pull = t {
                            got_pull.store(true, AtomicOrdering::Release);
                            if !in_loop.load(AtomicOrdering::Acquire)
                                && !res_done.load(AtomicOrdering::Acquire)
                            {
                                r#loop();
                            }
                        } else if let Message::End = t {
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
