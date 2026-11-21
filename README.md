# async-executor

[![Build](https://github.com/smol-rs/async-executor/actions/workflows/ci.yml/badge.svg)](
https://github.com/smol-rs/async-executor/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0_OR_MIT-blue.svg)](
https://github.com/smol-rs/async-executor)
[![Cargo](https://img.shields.io/crates/v/async-executor.svg)](
https://crates.io/crates/async-executor)
[![Documentation](https://docs.rs/async-executor/badge.svg)](
https://docs.rs/async-executor)

Async executors.

This crate provides two reference executors that trade performance for
functionality. They should be considered reference executors that are "good
enough" for most use cases. For more specialized use cases, consider writing
your own executor on top of [`async-task`].

[`async-task`]: https://crates.io/crates/async-task

## Examples

```rust
use async_executor::Executor;
use futures_lite::future;

// Create a new executor.
let ex = Executor::new();

// Spawn a task.
let task = ex.spawn(async {
    println!("Hello world");
});

// Run the executor until the task completes.
future::block_on(ex.run(task));
```

## Automatic blocking-task isolation

Driving an [`Executor`] with a [`WorkerPool`] keeps one async worker per
logical CPU by default. If a task performs a synchronous blocking call (a
plain `std::thread::sleep`, a blocking syscall, etc.), the pool notices that
the poll is taking too long, classifies that task as blocking without any
cooperation from the caller, and moves it onto a dedicated blocking thread.
A replacement async worker is spawned immediately, so a single blocking task
can no longer drag the executor's concurrency down to one. Ordinary async
tasks keep their original scheduling path and pay only a single relaxed
atomic load per reschedule.

```rust
use std::sync::Arc;
use std::time::Duration;

use async_executor::{Executor, WorkerPool};

let executor = Arc::new(Executor::new());
let _pool = WorkerPool::new(executor.clone());

// This blocking task is detected and isolated automatically.
let task = executor.spawn(async {
    std::thread::sleep(Duration::from_millis(50));
    42
});

assert_eq!(futures_lite::future::block_on(task), 42);
```

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
