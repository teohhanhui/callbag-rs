use arc_swap::{ArcSwap, ArcSwapOption};
use paste::paste;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};

use crate::{
    utils::{
        call,
        tracing::{instrument, trace},
    },
    Message, Source,
};

#[cfg(feature = "tracing")]
use {std::fmt, tracing::Span};

/// Callbag factory that combines the latest data points from multiple (2 or more) callbag sources.
///
/// It delivers those latest values as a tuple.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/readme.js#L32-L71>
///
/// # Examples
///
/// ```no_run
/// use async_executors::TimerExt;
/// use async_nursery::Nursery;
/// use crossbeam_queue::SegQueue;
/// use std::{sync::Arc, time::Duration};
///
/// use callbag::{combine, for_each, interval};
///
/// let (nursery, nursery_out) = Nursery::new(async_executors::AsyncStd);
///
/// let actual = Arc::new(SegQueue::new());
///
/// let source = combine!(
///     interval(Duration::from_millis(100), nursery.clone()),
///     interval(Duration::from_millis(350), nursery.clone()),
/// );
///
/// for_each({
///     let actual = Arc::clone(&actual);
///     move |x| {
///         println!("{x:?}");
///         actual.push(x);
///     }
/// })(source);
///
/// let nursery_out = nursery.timeout(Duration::from_millis(1_000), nursery_out);
/// drop(nursery);
/// async_std::task::block_on(nursery_out);
///
/// assert_eq!(
///     &{
///         let mut v = vec![];
///         while let Some(x) = actual.pop() {
///             v.push(x);
///         }
///         v
///     }[..],
///     [
///         (2, 0),
///         (3, 0),
///         (4, 0),
///         (5, 0),
///         (6, 0),
///         (6, 1),
///         (7, 1),
///         (8, 1),
///     ]
/// );
/// ```
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
    )+) => (paste! {
        $(
            impl<$($T),+> Unwrap for ($(Option<$T>,)+) {
                type Output = ($($T,)+);

                fn unwrap(self) -> Self::Output {
                    ($(self.$idx.unwrap(),)+)
                }
            }

            impl<$(
                #[cfg(not(feature = "tracing"))] $T: 'static,
                #[cfg(feature = "tracing")] $T: fmt::Debug + 'static,
                #[cfg(not(feature = "tracing"))] [<S $T>]: 'static,
                #[cfg(feature = "tracing")] [<S $T>]: fmt::Debug + 'static,
            )+> Combine for ($([<S $T>],)+)
            where
                $(
                    $T: Clone + Send + Sync,
                    [<S $T>]: IntoArcSource<Output = $T> + Send + Sync,
                )+
            {
                type Output = ($($T,)+);

                #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
                fn combine(self) -> Source<Self::Output> {
                    #[cfg(feature = "tracing")]
                    let combine_fn_span = Span::current();
                    $(
                        let [<source_ $idx>] = self.$idx.into_arc_source();
                    )+
                    (move |message| {
                        instrument!(
                            follows_from: &combine_fn_span,
                            "combine",
                            combine_span
                        );
                        trace!("from sink: {message:?}");
                        if let Message::Handshake(sink) = message {
                            const N: usize = last_literal!($($idx,)+) + 1;
                            let n_start = Arc::new(AtomicUsize::new(N));
                            let n_data = Arc::new(AtomicUsize::new(N));
                            let n_end = Arc::new(AtomicUsize::new(N));
                            let vals: Arc<ArcSwap<($(Option<$T>,)+)>> =
                                Arc::new(Default::default());
                            let source_talkbacks: Arc<($(ArcSwapOption<Source<$T>>,)+)> =
                                Arc::new(Default::default());
                            let talkback: Arc<Source<Self::Output>> = Arc::new(
                                {
                                    #[cfg(feature = "tracing")]
                                    let combine_span = combine_span.clone();
                                    let source_talkbacks = Arc::clone(&source_talkbacks);
                                    move |message| {
                                        instrument!(
                                            parent: &combine_span,
                                            "sink_talkback"
                                        );
                                        trace!("from sink: {message:?}");
                                        match message {
                                            Message::Handshake(_) => {
                                                panic!("sink handshake has already occurred");
                                            }
                                            Message::Data(_) => {
                                                panic!("sink must not send data");
                                            }
                                            Message::Pull => {
                                                $(
                                                    let source_talkback =
                                                        source_talkbacks.$idx.load();
                                                    let source_talkback = source_talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    call!(
                                                        source_talkback,
                                                        Message::Pull,
                                                        "to source {i}: {message:?}",
                                                        i = $idx,
                                                    );
                                                )+
                                            }
                                            Message::Error(ref error) => {
                                                $(
                                                    let source_talkback =
                                                        source_talkbacks.$idx.load();
                                                    let source_talkback = source_talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    call!(
                                                        source_talkback,
                                                        Message::Error(Arc::clone(error)),
                                                        "to source {i}: {message:?}",
                                                        i = $idx,
                                                    );
                                                )+
                                            }
                                            Message::Terminate => {
                                                $(
                                                    let source_talkback =
                                                        source_talkbacks.$idx.load();
                                                    let source_talkback = source_talkback
                                                        .as_ref()
                                                        .expect("source talkback not set");
                                                    call!(
                                                        source_talkback,
                                                        Message::Terminate,
                                                        "to source {i}: {message:?}",
                                                        i = $idx,
                                                    );
                                                )+
                                            }
                                        }
                                    }
                                }
                                .into(),
                            );
                            $(
                                call!(
                                    [<source_ $idx>],
                                    Message::Handshake(Arc::new(
                                        {
                                            #[cfg(feature = "tracing")]
                                            let combine_span = combine_span.clone();
                                            let sink = Arc::clone(&sink);
                                            let n_start = Arc::clone(&n_start);
                                            let n_data = Arc::clone(&n_data);
                                            let n_end = Arc::clone(&n_end);
                                            let vals = Arc::clone(&vals);
                                            let source_talkbacks = Arc::clone(&source_talkbacks);
                                            let talkback = Arc::clone(&talkback);
                                            move |message| {
                                                instrument!(
                                                    parent: &combine_span,
                                                    "source_talkback"
                                                );
                                                trace!("from source {i}: {message:?}", i = $idx);
                                                match message {
                                                    Message::Handshake(source) => {
                                                        source_talkbacks.$idx.store(Some(source));
                                                        let n_start = n_start
                                                            .fetch_sub(1, AtomicOrdering::AcqRel)
                                                            - 1;
                                                        if n_start == 0 {
                                                            call!(
                                                                sink,
                                                                Message::Handshake(Arc::clone(
                                                                    &talkback
                                                                )),
                                                                "to sink: {message:?}"
                                                            );
                                                        }
                                                    }
                                                    Message::Data(data) => {
                                                        let n_data = if vals
                                                            .load()
                                                            .$idx
                                                            .is_none()
                                                        {
                                                            n_data.fetch_sub(
                                                                1,
                                                                AtomicOrdering::AcqRel,
                                                            ) - 1
                                                        } else {
                                                            n_data.load(
                                                                AtomicOrdering::Acquire,
                                                            )
                                                        };
                                                        vals.rcu(move |vals| {
                                                            let mut vals = (**vals).clone();
                                                            vals.$idx = Some(data.clone());
                                                            vals
                                                        });
                                                        if n_data == 0 {
                                                            call!(
                                                                sink,
                                                                Message::Data(
                                                                    (**vals.load())
                                                                        .clone()
                                                                        .unwrap(),
                                                                ),
                                                                "to sink: {message:?}"
                                                            );
                                                        }
                                                    }
                                                    Message::Pull => {
                                                        panic!("source must not pull");
                                                    }
                                                    Message::Error(_) | Message::Terminate => {
                                                    let n_end = n_end
                                                        .fetch_sub(1, AtomicOrdering::AcqRel)
                                                        - 1;
                                                        if n_end == 0 {
                                                            call!(
                                                                sink,
                                                                Message::Terminate,
                                                                "to sink: {message:?}"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        .into(),
                                    )),
                                    "to source {i}: {message:?}",
                                    i = $idx,
                                );
                            )+
                        }
                    })
                    .into()
                }
            }
        )+
    });
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

pub trait IntoArcSource {
    type Output;

    fn into_arc_source(self) -> Arc<Source<Self::Output>>;
}

trait Unwrap {
    type Output;

    fn unwrap(self) -> Self::Output;
}

impl<T> IntoArcSource for Arc<Source<T>> {
    type Output = T;

    fn into_arc_source(self) -> Arc<Source<Self::Output>> {
        self
    }
}

impl<T> IntoArcSource for Source<T> {
    type Output = T;

    fn into_arc_source(self) -> Arc<Source<Self::Output>> {
        Arc::new(self)
    }
}

impl<T> IntoArcSource for Box<Source<T>> {
    type Output = T;

    fn into_arc_source(self) -> Arc<Source<Self::Output>> {
        Arc::from(self)
    }
}

/// Callbag factory that combines the latest data points from multiple (2 or more) callbag sources.
///
/// It delivers those latest values as a tuple.
///
/// Works with both pullable and listenable sources.
///
/// See <https://github.com/staltz/callbag-combine/blob/44b4f0f4295e0f5f9dbe9610d0548beca93fe376/readme.js#L32-L71>
///
/// # Implementation notes
///
/// Due to a temporary restriction in Rust’s type system, the `Combine` trait is only implemented
/// on tuples of arity 12 or less.
#[doc(hidden)]
pub fn combine<T>(sources: T) -> Source<<T as Combine>::Output>
where
    T: Combine,
{
    sources.combine()
}
