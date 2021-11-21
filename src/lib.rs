use never::Never;
use std::ops::Deref;

pub use crate::filter::filter;
pub use crate::for_each::for_each;
pub use crate::from_iter::from_iter;
pub use crate::map::map;
pub use crate::scan::scan;

mod filter;
mod for_each;
mod from_iter;
mod map;
mod scan;

/// A message passed to a [`Callbag`].
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L12-L22>
pub enum Message<I, O> {
    Handshake(Callbag<O, I>),
    Data(I),
    Pull,
    Terminate,
}

/// A `Callbag` dynamically receives input of type `I` and dynamically delivers output of type `O`.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L24-L30>
pub struct Callbag<I, O>(Box<dyn Fn(Message<I, O>) + Send + Sync>);

/// A source only delivers data.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L32-L35>
pub type Source<T> = Callbag<Never, T>;

/// A sink only receives data.
///
/// See <https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L37-L40>
pub type Sink<T> = Callbag<T, Never>;

impl<I, O, F> From<F> for Callbag<I, O>
where
    F: 'static + Fn(Message<I, O>) + Send + Sync,
{
    fn from(handler: F) -> Self {
        Callbag(Box::new(handler))
    }
}

impl<I, O> Deref for Callbag<I, O> {
    type Target = Box<dyn Fn(Message<I, O>) + Send + Sync>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
