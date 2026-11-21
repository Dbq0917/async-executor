use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_executor::{Executor, WorkerPool};

#[test]
fn pool_runs_plain_tasks() {
    let executor = Arc::new(Executor::new());
    let _pool = WorkerPool::new(executor.clone());

    let task = executor.spawn(async { 1 + 2 });
    assert_eq!(futures_lite::future::block_on(task), 3);
}

#[test]
fn pool_runs_timer_task() {
    let executor = Arc::new(Executor::new());
    let _pool = WorkerPool::new(executor.clone());

    let task = executor.spawn(async {
        async_io::Timer::after(Duration::from_millis(20)).await;
        7
    });
    assert_eq!(futures_lite::future::block_on(task), 7);
}

#[test]
fn pool_handles_one_blocking_sleep() {
    let executor = Arc::new(Executor::new());
    let pool = WorkerPool::new(executor.clone());

    let task = executor.spawn(async {
        thread::sleep(Duration::from_millis(100));
        9
    });
    assert_eq!(futures_lite::future::block_on(task), 9);
    assert!(pool.blocked_task_count() >= 1);
}

#[test]
fn pool_shuts_down_even_with_pending_work() {
    let executor = Arc::new(Executor::new());
    let pool = WorkerPool::with_worker_count(executor.clone(), 4);

    // A long-lived blocking task and some async timers are still registered
    // when the pool goes out of scope; dropping it must return promptly.
    let _long_blocker = executor.spawn(async {
        thread::sleep(Duration::from_secs(60));
    });
    for _ in 0..8 {
        executor
            .spawn(async {
                async_io::Timer::after(Duration::from_secs(60)).await;
            })
            .detach();
    }

    // Let the blocker get detected before shutting down.
    thread::sleep(Duration::from_millis(40));

    let started = std::time::Instant::now();
    drop(pool);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "pool drop hung for {:?}",
        started.elapsed()
    );
}

#[test]
fn multiple_blocking_tasks_run_in_parallel() {
    let executor = Arc::new(Executor::new());
    let pool = WorkerPool::with_worker_count(executor.clone(), 2);

    let blockers: usize = 4;
    let per_task = Duration::from_millis(200);

    let mut tasks = Vec::new();
    for _ in 0..blockers {
        tasks.push(executor.spawn(async move {
            // A long synchronous block gets the task flagged as blocking; the
            // following yields force reschedules after the flag is set, so the
            // later polls are routed through the dedicated blocking queue.
            futures_lite::future::yield_now().await;
            thread::sleep(per_task);
            futures_lite::future::yield_now().await;
            futures_lite::future::yield_now().await;
        }));
    }

    let start = std::time::Instant::now();
    for task in tasks {
        futures_lite::future::block_on(task);
    }
    let elapsed = start.elapsed();

    // All four blockers were isolated onto blocking threads, so they overlap
    // instead of being serialized behind the two async workers.
    assert!(
        elapsed < per_task * blockers as u32,
        "blocking tasks were serialized: {elapsed:?}"
    );
    assert!(pool.blocked_task_count() >= blockers);
    // Every blocker reschedules after being flagged, so its later poll runs on
    // a dedicated blocking thread rather than an async worker.
    assert!(pool.isolated_run_count() >= 1);
}
