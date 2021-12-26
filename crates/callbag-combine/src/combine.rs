use arc_swap::{ArcSwap, ArcSwapOption};
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use callbag_core::{Message, Source};

/// Callbag factory that combines the latest data points from multiple (2 or more) callbag sources.
///
/// It delivers those latest values as a tuple.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-combine/blob/912a5ec8ec3d9e65d3beccdc7a53eabd624c1c8a/readme.js#L32-L71>
///
/// # Implementation notes
///
/// Due to a temporary restriction in Rust’s type system, the `Combine` trait is only implemented
/// on tuples of arity 12 or less.
#[macro_export]
macro_rules! combine {
    ($($s:expr),+ $(,)?) => {
        $crate::combine(($($s,)+))
    };
}

macro_rules! combine_impls {
    ($(
        $Combine:ident {
            $(($idx:tt) -> $T:ident)+
        }
    )+) => {
        $(
            impl<$($T),+> Unwrap for ($(Option<$T>,)+) {
                type Output = ($($T,)+);

                fn unwrap(self) -> Self::Output {
                    ($(self.$idx.unwrap(),)+)
                }
            }

            impl<$($T: Send + Sync + Clone + 'static),+> Combine for ($(Arc<Source<$T>>,)+) {
                type Output = ($($T,)+);

                fn combine(self) -> Source<Self::Output> {
                    (move |message| {
                        if let Message::Handshake(sink) = message {
                            const N: usize = last_literal!($($idx,)+) + 1;
                            let n_start = Arc::new(AtomicUsize::new(N));
                            let n_data = Arc::new(AtomicUsize::new(N));
                            let n_end = Arc::new(AtomicUsize::new(N));
                            let vals: Arc<ArcSwap<($(Option<$T>,)+)>> = Arc::new(Default::default());
                            let source_talkbacks: Arc<($(ArcSwapOption<Source<$T>>,)+)> = Arc::new(Default::default());
                            let talkback: Arc<Source<Self::Output>> = Arc::new(
                                {
                                    let source_talkbacks = Arc::clone(&source_talkbacks);
                                    move |message| match message {
                                        Message::Handshake(_) => {
                                            panic!("sink handshake has already occurred");
                                        }
                                        Message::Data(_) => {
                                            panic!("sink must not send data");
                                        }
                                        Message::Pull => {
                                            $({
                                                let source_talkback = source_talkbacks.$idx.load();
                                                let source_talkback = source_talkback.as_ref().unwrap();
                                                source_talkback(Message::Pull);
                                            })+
                                        }
                                        Message::Error(ref error) => {
                                            $({
                                                let source_talkback = source_talkbacks.$idx.load();
                                                let source_talkback = source_talkback.as_ref().unwrap();
                                                source_talkback(Message::Error(error.clone()));
                                            })+
                                        }
                                        Message::Terminate => {
                                            $({
                                                let source_talkback = source_talkbacks.$idx.load();
                                                let source_talkback = source_talkback.as_ref().unwrap();
                                                source_talkback(Message::Terminate);
                                            })+
                                        }
                                    }
                                }
                                .into()
                            );
                            $(
                                (self.$idx)(Message::Handshake(Arc::new(
                                    {
                                        let sink = Arc::clone(&sink);
                                        let n_start = Arc::clone(&n_start);
                                        let n_data = Arc::clone(&n_data);
                                        let n_end = Arc::clone(&n_end);
                                        let vals = Arc::clone(&vals);
                                        let source_talkbacks = Arc::clone(&source_talkbacks);
                                        let talkback = Arc::clone(&talkback);
                                        move |message| match message {
                                            Message::Handshake(source) => {
                                                source_talkbacks.$idx.store(Some(source));
                                                let n_start = n_start.fetch_sub(1, AtomicOrdering::AcqRel) - 1;
                                                if n_start == 0 {
                                                    sink(Message::Handshake(Arc::clone(&talkback)));
                                                }
                                            }
                                            Message::Data(data) => {
                                                let n_data = if vals.load().$idx.is_none() {
                                                    n_data.fetch_sub(1, AtomicOrdering::AcqRel) - 1
                                                } else {
                                                    n_data.load(AtomicOrdering::Acquire)
                                                };
                                                vals.rcu(move |vals| {
                                                    let mut vals = (**vals).clone();
                                                    vals.$idx = Some(data.clone());
                                                    vals
                                                });
                                                if n_data == 0 {
                                                    sink(Message::Data((**vals.load()).clone().unwrap()));
                                                }
                                            }
                                            Message::Pull => {
                                                panic!("source must not pull");
                                            }
                                            Message::Error(_) | Message::Terminate => {
                                                let n_end = n_end.fetch_sub(1, AtomicOrdering::AcqRel) - 1;
                                                if n_end == 0 {
                                                    sink(Message::Terminate);
                                                }
                                            }
                                        }
                                    }
                                    .into()
                                )));
                            )+
                        }
                    })
                    .into()
                }
            }
        )+
    };
}

macro_rules! last_literal {
    ($a:literal,) => { $a };
    ($a:literal, $($rest_a:literal,)+) => { last_literal!($($rest_a,)+) };
}

combine_impls! {
    Combine1 {
        (0) -> A
    }
    Combine2 {
        (0) -> A
        (1) -> B
    }
    Combine3 {
        (0) -> A
        (1) -> B
        (2) -> C
    }
    Combine4 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
    }
    Combine5 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
    }
    Combine6 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
    }
    Combine7 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
    }
    Combine8 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
    }
    Combine9 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
    }
    Combine10 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
    }
    Combine11 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
    }
    Combine12 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
    }
}

pub trait Combine {
    type Output;

    fn combine(self) -> Source<Self::Output>;
}

trait Unwrap {
    type Output;

    fn unwrap(self) -> Self::Output;
}

/// Callbag factory that combines the latest data points from multiple (2 or more) callbag sources.
///
/// It delivers those latest values as a tuple.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-combine/blob/912a5ec8ec3d9e65d3beccdc7a53eabd624c1c8a/readme.js#L32-L71>
///
/// # Implementation notes
///
/// Due to a temporary restriction in Rust’s type system, the `Combine` trait is only implemented
/// on tuples of arity 12 or less.
pub fn combine<T: Combine>(sources: T) -> Source<<T as Combine>::Output> {
    sources.combine()
}
