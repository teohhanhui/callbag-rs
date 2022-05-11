macro_rules! instrument {
    (parent: $parent:expr, $name:expr, $span:ident) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                ::paste::paste! {
                    let $span = ::tracing::trace_span!(parent: $parent, $name);
                    let [<_ $name _entered>] = $span.enter();
                }
            }
        }
    };
    (parent: $parent:expr, $name:expr) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                ::paste::paste! {
                    let [<_ $name _entered>] = ::tracing::trace_span!(parent: $parent, $name).entered();
                }
            }
        }
    };
    (follows_from: $follows_from:expr, $name:expr, $span:ident) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                ::paste::paste! {
                    let $span = ::tracing::trace_span!($name);
                    $span.follows_from($follows_from);
                    let [<_ $name _entered>] = $span.enter();
                }
            }
        }
    };
    (follows_from: $follows_from:expr, $name:expr) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                ::paste::paste! {
                    let [<$name _span>] = ::tracing::trace_span!($name);
                    [<$name _span>].follows_from($follows_from);
                    let [<_ $name _entered>] = [<$name _span>].enter();
                }
            }
        }
    };
}
pub(crate) use instrument;

macro_rules! trace {
    ($($arg:tt)+) => {
        ::cfg_if::cfg_if! {
            if #[cfg(feature = "tracing")] {
                ::tracing::trace!($($arg)+);
            }
        }
    };
}
pub(crate) use trace;
