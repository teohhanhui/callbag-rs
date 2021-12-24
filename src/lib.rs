use never::Never;
use std::{
    error::Error,
    fmt::{self, Debug},
    ops::Deref,
    sync::Arc,
};

pub use crate::combine::combine;
pub use crate::concat::concat;
pub use crate::filter::filter;
pub use crate::flatten::flatten;
pub use crate::for_each::for_each;
pub use crate::from_iter::from_iter;
pub use crate::map::map;
pub use crate::merge::merge;
pub use crate::scan::scan;
pub use crate::share::share;
pub use crate::skip::skip;
pub use crate::take::take;

mod combine;
mod concat;
mod filter;
mod flatten;
mod for_each;
mod from_iter;
mod map;
mod merge;
mod pipe;
mod scan;
mod share;
mod skip;
mod take;

/// A message passed to a [`Callbag`].
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L12-L22>
#[derive(Clone, Debug)]
pub enum Message<I, O> {
    Handshake(Arc<Callbag<O, I>>),
    Data(I),
    Pull,
    Error(Arc<dyn Error + Send + Sync + 'static>),
    Terminate,
}

/// A `Callbag` dynamically receives input of type `I` and dynamically delivers output of type `O`.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L24-L30>
pub struct Callbag<I, O>(CallbagFn<I, O>);

/// A source only delivers data.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L32-L35>
pub type Source<T> = Callbag<Never, T>;

/// A sink only receives data.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L37-L40>
pub type Sink<T> = Callbag<T, Never>;

pub type CallbagFn<I, O> = Box<dyn Fn(Message<I, O>) + Send + Sync>;

impl<I, O> Deref for Callbag<I, O> {
    type Target = CallbagFn<I, O>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I, O> Debug for Callbag<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Callbag<{}, {}>",
            std::any::type_name::<I>(),
            std::any::type_name::<O>(),
        )
    }
}

impl<I, O, F: 'static> From<F> for Callbag<I, O>
where
    F: Fn(Message<I, O>) + Send + Sync,
{
    fn from(handler: F) -> Self {
        Callbag(Box::new(handler))
    }
}
