pub mod tracing;

#[allow(unused_macros)]
macro_rules! call {
    ($callbag:expr, $message:expr, $str:literal) => {
        {
            let message = $message;
            ::tracing::info!($str, message = message);
            $callbag(message);
        }
    };
    ($callbag:expr, $message:expr, $str:literal, $($arg:tt)+) => {
        {
            let message = $message;
            ::tracing::info!($str, message = message, $($arg)+);
            $callbag(message);
        }
    };
}
#[allow(unused_imports)]
pub(crate) use call;
