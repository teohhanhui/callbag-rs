#   Changelog

##  Unreleased

### Added

-   Add `tracing` feature to crate

##  v0.14.0 - 2022-01-14

### Changed

-   Accept `Source<T>` and `Box<Source<T>>` in addition to `Arc<Source<T>>` in `combine` operator
-   Emit `Message::Error(_)` in `interval` function if `Nursery::nurse` call errors

### Breaking changes

-   Upgrade `async_nursery`
-   Change trait bounds on `nursery` parameter of `interval` function

##  v0.13.0 - 2021-12-28

### Added

-   Implement `interval`

### Changed

-   Add a feature gate for each function

##  v0.12.0 - 2021-12-25

### Added

-   Implement `concat` operator

##  v0.11.0 - 2021-12-24

### Added

-   Implement `combine` operator

##  v0.10.0 - 2021-12-16

### Added

-   Implement `share`

### Breaking changes

-   Change type of parameter accepted by `merge` function

##  v0.9.0 - 2021-12-14

### Added

-   Implement `pipe`
-   Implement `merge` operator

### Changed

-   Change `Debug::fmt` output of `Callbag` type
-   Implement `Clone` for `Message` type

### Breaking changes

-   Change `Message::Error(_)` variant
-   Change `Message::Handshake(_)` variant

##  v0.8.0 - 2021-12-07

### Changed

-   Implement `Debug` for `Callbag` and `Message` types

##  v0.7.0 - 2021-12-06

### Added

-   Implement `take` operator
-   Implement `skip` operator

##  v0.6.0 - 2021-11-24

### Added

-   Implement `flatten` operator

### Breaking changes

-   Add `Message::Error(_)` variant

##  v0.5.0 - 2021-11-22

### Added

-   Implement `filter` operator

##  v0.4.0 - 2021-11-22

### Added

-   Implement `scan` operator

##  v0.3.0 - 2021-11-09

### Added

-   Implement `map` operator

##  v0.2.0 - 2021-11-04

### Added

-   Implement `for_each`

### Breaking changes

-   Adjust types

##  v0.1.0 - 2021-11-04

### Added

-   Implement `from_iter`
