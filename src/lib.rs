use never::Never;
use std::ops::Deref;

pub use crate::from_iter::from_iter;

mod from_iter;

/// https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L12-L22
pub enum Message<I, O> {
    Handshake(Callbag<O, I>),
    Data(I),
    Pull,
    End,
}

/// A Callbag dynamically receives input of type I and dynamically delivers output of type O
///
/// https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L24-L30
pub struct Callbag<I, O>(Box<dyn Fn(Message<I, O>) + Send + Sync>);

/// A source only delivers data
///
/// https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L32-L35
pub type Source<T> = Callbag<Never, T>;

/// A sink only receives data
///
/// https://github.com/callbag/callbag/blob/9020d6f68f31034a717465dce38235df749f3353/types.d.ts#L37-L40
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
