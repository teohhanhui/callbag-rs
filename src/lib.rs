use std::ops::Deref;

pub use crate::from_iter::from_iter;

mod from_iter;

pub struct Callbag<T>(Box<dyn Fn(Message<T>) + Send + Sync>);
/// A source only delivers data
pub type Source<T> = Callbag<T>;
/// A sink only receives data
pub type Sink<T> = Callbag<T>;

pub enum Message<T> {
    Handshake(Callbag<T>),
    Data(T),
    Pull,
    End,
}

impl<T, F> From<F> for Callbag<T>
where
    F: 'static + Fn(Message<T>) + Send + Sync,
{
    fn from(handler: F) -> Self {
        Callbag(Box::new(handler))
    }
}

impl<T> Deref for Callbag<T> {
    type Target = Box<dyn Fn(Message<T>) + Send + Sync>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
