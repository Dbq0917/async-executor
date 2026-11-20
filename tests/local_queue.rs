use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use async_executor::Executor;
use futures_lite::{future, pin};

#[test]
fn two_queues() {
    future::block_on(async {
        // Create an executor with two runners.
        let ex = Executor::new();
        let (run1, run2) = (
            ex.run(future::pending::<()>()),
            ex.run(future::pending::<()>()),
        );
        let mut run1 = Box::pin(run1);
        pin!(run2);

        // Poll them both.
        assert!(future::poll_once(run1.as_mut()).await.is_none());
        assert!(future::poll_once(run2.as_mut()).await.is_none());

        // Drop the first one, which should leave the local queue in the `None` state.
        drop(run1);
        assert!(future::poll_once(run2.as_mut()).await.is_none());
    });
}

#[test]
fn short_tasks_do_not_wait_for_long_task() {
    const SHORT_TASKS: usize = 64;
    const LONG_TASK_DURATION: Duration = Duration::from_millis(200);
    const MAX_SHORT_TASK_LATENCY: Duration = Duration::from_millis(100);

    let ex = Arc::new(Executor::new());
    let long_task_started = Arc::new(Barrier::new(2));
    let completed = Arc::new(AtomicUsize::new(0));

    // Submit the long task first so that the runner that picks it up also
    // hoists the following short tasks into its local queue.
    ex.spawn({
        let long_task_started = long_task_started.clone();

        async move {
            long_task_started.wait();
            thread::sleep(LONG_TASK_DURATION);
        }
    })
    .detach();

    for _ in 0..SHORT_TASKS {
        ex.spawn({
            let completed = completed.clone();

            async move {
                completed.fetch_add(1, Ordering::SeqCst);
            }
        })
        .detach();
    }

    let (signal, shutdown) = async_channel::bounded::<()>(1);
    let runner = {
        let ex = ex.clone();
        thread::spawn(move || future::block_on(ex.run(shutdown.recv())))
    };

    // Wait until the long task is running. At this point its local queue has
    // already been filled with short tasks.
    long_task_started.wait();

    let start = Instant::now();
    while completed.load(Ordering::SeqCst) < SHORT_TASKS {
        if !ex.try_tick() {
            thread::yield_now();
        }
    }
    let short_task_latency = start.elapsed();

    drop(signal);
    let _ = runner.join().unwrap();

    assert!(
        short_task_latency < MAX_SHORT_TASK_LATENCY,
        "short tasks took {short_task_latency:?}, while the long task ran for {LONG_TASK_DURATION:?}",
    );
}
