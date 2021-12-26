/// Utility function for plugging callbags together in chain.
///
/// This utility actually doesn't rely on Callbag specifics, and is really similar to
/// [Ramda's `pipe`][ramda-pipe] or [lodash's `flow`][lodash-flow].
///
/// This exists to play nicely with the ecosystem, and to facilitate the import of the function.
///
/// See <https://github.com/staltz/callbag-pipe/blob/a2e5b985ce7aa55de2749e1c3e08867f45edc6fa/readme.js#L110-L114>
///
/// [lodash-flow]: https://lodash.com/docs/#flow
/// [ramda-pipe]: https://ramdajs.com/docs/#pipe
#[macro_export]
macro_rules! pipe {
    ($a:expr, $b:expr $(,)?) => { $b($a) };
    ($a:expr, $b:expr, $($rest:expr),* $(,)?) => {
        $crate::pipe!($b($a), $($rest),*)
    };
}