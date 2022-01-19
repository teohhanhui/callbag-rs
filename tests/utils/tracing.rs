#[allow(unused_macros)]
macro_rules! instrument {
    (parent: $parent:expr, $name:expr, $span:ident) => {
        ::paste::paste! {
            let $span = ::tracing::info_span!(parent: $parent, $name);
            let [<_ $name _entered>] = $span.enter();
        }
    };
    (parent: $parent:expr, $name:expr) => {
        ::paste::paste! {
            let [<_ $name _entered>] = ::tracing::info_span!(parent: $parent, $name).entered();
        }
    };
    (follows_from: $follows_from:expr, $name:expr, $span:ident) => {
        ::paste::paste! {
            let $span = ::tracing::info_span!($name);
            $span.follows_from($follows_from);
            let [<_ $name _entered>] = $span.enter();
        }
    };
    (follows_from: $follows_from:expr, $name:expr) => {
        ::paste::paste! {
            let [<$name _span>] = ::tracing::info_span!($name);
            [<$name _span>].follows_from($follows_from);
            let [<_ $name _entered>] = [<$name _span>].enter();
        }
    };
}
#[allow(unused_imports)]
pub(crate) use instrument;
