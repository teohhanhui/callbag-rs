pub(crate) mod tracing;

macro_rules! call {
    ($callbag:expr, $message:expr, $str:literal) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                {
                    let message = $message;
                    ::tracing::trace!($str, message = message);
                    $callbag(message);
                }
            } else {
                $callbag($message);
            }
        }
    };
    ($callbag:expr, $message:expr, $str:literal, $($arg:tt)+) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                {
                    let message = $message;
                    ::tracing::trace!($str, message = message, $($arg)+);
                    $callbag(message);
                }
            } else {
                $callbag($message);
            }
        }
    };
}
pub(crate) use call;
