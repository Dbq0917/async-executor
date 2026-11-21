use async_executor::{Executor, LocalExecutor, WorkerPool};
use futures_lite::future;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(not(miri))]
const READY_COUNT: usize = 50_000;
#[cfg(miri)]
const READY_COUNT: usize = 505;

#[test]
fn spawn_many() {
    future::block_on(async {
        let ex = Executor::new();

        // Spawn a lot of tasks.
        let mut tasks = vec![];
        ex.spawn_many((0..READY_COUNT).map(future::ready), &mut tasks);

        // Run all of the tasks in parallel.
        ex.run(async move {
            for (i, task) in tasks.into_iter().enumerate() {
                assert_eq!(task.await, i);
            }
        })
        .await;
    });
}

#[test]
fn spawn_many_local() {
    future::block_on(async {
        let ex = LocalExecutor::new();

        // Spawn a lot of tasks.
        let mut tasks = vec![];
        ex.spawn_many((0..READY_COUNT).map(future::ready), &mut tasks);

        // Run all of the tasks in parallel.
        ex.run(async move {
            for (i, task) in tasks.into_iter().enumerate() {
                assert_eq!(task.await, i);
            }
        })
        .await;
    });
}

/// Mixing a blocking task into the normal scheduling workload must not drop
/// the executor's async concurrency.
///
/// The blocking call is a plain synchronous sleep with no marker or wrapper,
/// so the pool can only keep concurrency up by detecting the block itself and
/// isolating the task onto a dedicated blocking thread. The blocker repeats its
/// synchronous sleep several times: with real isolation it is detected once
/// and every later poll runs on a blocking thread, while without isolation it
/// would re-block an async worker on every iteration and starve the pool.
#[test]
fn blocking_task_does_not_kill_concurrency() {
    // A deliberately small pool makes the concurrency loss easy to observe.
    let workers = 2;
    let executor = Arc::new(Executor::new());
    let pool = WorkerPool::with_worker_count(executor.clone(), workers);
    assert_eq!(pool.worker_count(), workers);

    const BLOCK_ROUNDS: usize = 4;
    const BLOCK_MS: u64 = 120;

    // The blocking task alternates an async yield (which reschedules it) with
    // a synchronous sleep (which blocks its worker thread).
    let blocker = executor.spawn(async {
        for round in 0..BLOCK_ROUNDS {
            futures_lite::future::yield_now().await;
            thread::sleep(Duration::from_millis(BLOCK_MS));
            let _ = round;
        }
        BLOCK_ROUNDS
    });

    // Probes continuously cycle through the pool and record how many run at the
    // same instant. With two healthy workers this must reach two even while the
    // blocker is active.
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let probe_rounds = Arc::new(AtomicUsize::new(0));

    let probe_loops = 8;
    let mut probes = Vec::new();
    for _ in 0..workers {
        let active = active.clone();
        let max_active = max_active.clone();
        let probe_rounds = probe_rounds.clone();
        probes.push(executor.spawn(async move {
            for _ in 0..probe_loops {
                active.fetch_add(1, Ordering::AcqRel);
                max_active.fetch_max(active.load(Ordering::Acquire), Ordering::AcqRel);
                async_io::Timer::after(Duration::from_millis(15)).await;
                active.fetch_sub(1, Ordering::AcqRel);
                probe_rounds.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }

    futures_lite::future::block_on(async {
        // The probes must all finish before the blocker would if the pool were
        // repeatedly losing a worker: each lost round costs BLOCK_MS and the
        // blocker alone occupies the pool for ~BLOCK_ROUNDS * BLOCK_MS.
        let timeout = async {
            async_io::Timer::after(Duration::from_millis(
                BLOCK_MS * BLOCK_ROUNDS as u64 - 50,
            ))
            .await;
        };
        let probes_fut = async {
            for probe in probes {
                probe.await;
            }
        };
        futures_lite::future::or(probes_fut, timeout).await;

        let done = probe_rounds.load(Ordering::Acquire);
        assert_eq!(
            done,
            workers * probe_loops,
            "only {done}/{} probe iterations finished: async concurrency collapsed behind the blocker",
            workers * probe_loops
        );

        assert_eq!(blocker.await, BLOCK_ROUNDS);
    });

    // The blocking task had to be recognized without any caller-provided tag.
    assert!(
        pool.blocked_task_count() >= 1,
        "watchdog never classified the synchronous sleep as blocking"
    );

    // The blocker rescheduled itself several times after being flagged, so its
    // later polls must have been routed onto a blocking thread. This is the
    // actual task-type isolation: thread injection alone leaves this at zero.
    assert!(
        pool.isolated_run_count() >= 1,
        "blocking task was never moved onto the dedicated blocking pool"
    );

    // Decisive invariant: two async probes overlapped while the blocker ran.
    assert!(
        max_active.load(Ordering::Acquire) >= 2,
        "async concurrency collapsed to one worker: max observed {}",
        max_active.load(Ordering::Acquire)
    );

    drop(pool);
}
