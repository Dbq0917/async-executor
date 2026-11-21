//! Reproduces the lost-wakeup symptom: after the worker goes idle, a single
//! task is produced at a time. If the enqueue/wake ordering loses the
//! notification, the task sits in the queue until the next task arrives, so
//! the measured queueing delay jumps from microseconds to roughly the gap
//! between productions (100 ms here).

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use async_io::Timer;
use async_executor::Executor;
use futures_lite::future;

const GAP: Duration = Duration::from_millis(100);

/// Upper bound for a healthy park/unpark round trip under scheduler noise.
/// A lost wakeup instead parks the worker for roughly `GAP` (100 ms).
const LATENCY_LIMIT: Duration = Duration::from_millis(75);

/// Bounds the blocking wait so a lost wakeup fails fast instead of hanging.
const WAIT_BUDGET: Duration = Duration::from_millis(85);

#[test]
fn single_task_wakeup_latency() {
    const ITERATIONS: usize = 100;

    let ex = Arc::new(Executor::new());
    let (shutdown_s, shutdown_r) = async_channel::bounded::<()>(1);

    // One worker thread so that every task must be picked up by the runner
    // after it has gone through the full idle/sleep transition.
    let worker = {
        let ex = ex.clone();
        thread::spawn(move || future::block_on(ex.run(shutdown_r.recv())))
    };

    // Give the worker time to reach the sleeping state before the first ping.
    thread::sleep(Duration::from_millis(200));

    let mut latencies = Vec::with_capacity(ITERATIONS);

    for _ in 0..ITERATIONS {
        let (s, r) = async_channel::bounded::<Instant>(1);
        let queued_at = Instant::now();
        ex.spawn(async move {
            let started = Instant::now();
            s.send(started).await.ok();
        })
        .detach();

        let latency = future::block_on(async {
            let timeout = async {
                Timer::after(WAIT_BUDGET).await;
                None
            };
            let recv = async { Some(r.recv().await.expect("channel closed")) };
            let started = future::or(core::pin::pin!(recv), core::pin::pin!(timeout))
                .await
                .expect("task was not picked up within wait budget: lost wakeup");
            started.duration_since(queued_at)
        });
        latencies.push(latency);

        // Let the runner drain and go back to sleep before the next ping.
        thread::sleep(GAP);
    }

    drop(shutdown_s);
    drop(ex);
    worker.join().unwrap().ok();

    let mut sorted = latencies;
    sorted.sort_unstable();
    let max = *sorted.last().unwrap();
    let median = sorted[sorted.len() / 2];

    println!("wakeup latency: median={:?} max={:?}", median, max);

    // A healthy wakeup path parks/unparks in well under a millisecond. If the
    // notification is ever lost, the task waits for the next production and
    // the delay lands near the 100 ms production gap.
    assert!(
        max < LATENCY_LIMIT,
        "lost wakeup detected: max queueing delay {:?}, all latencies: {:?}",
        max,
        sorted
    );
}

#[test]
fn burst_wakeup_latency() {
    const ROUNDS: usize = 25;
    const BATCH: usize = 50;

    let ex = Arc::new(Executor::new());
    let (shutdown_s, shutdown_r) = async_channel::bounded::<()>(1);

    let worker = {
        let ex = ex.clone();
        thread::spawn(move || future::block_on(ex.run(shutdown_r.recv())))
    };

    thread::sleep(Duration::from_millis(200));

    let mut max_latency = Duration::ZERO;

    for _ in 0..ROUNDS {
        // Fully drain between rounds so the runner is asleep when the batch
        // lands; the first task of each batch therefore relies on the wakeup.
        thread::sleep(GAP);

        let (s, r) = async_channel::bounded::<(usize, Instant)>(BATCH);
        let round_start = Instant::now();

        for i in 0..BATCH {
            let s = s.clone();
            ex.spawn(async move {
                s.send((i, Instant::now())).await.ok();
            })
            .detach();
        }
        drop(s);

        let collect = async {
            let mut received = 0;
            while let Ok((_, started)) = r.recv().await {
                max_latency = max_latency.max(started.duration_since(round_start));
                received += 1;
            }
            assert_eq!(received, BATCH);
        };
        let collect = core::pin::pin!(collect);

        let timeout = async {
            Timer::after(WAIT_BUDGET).await;
            panic!("batch did not drain within wait budget: lost wakeup");
        };
        let timeout = core::pin::pin!(timeout);

        future::block_on(future::or(collect, timeout));
    }

    drop(shutdown_s);
    drop(ex);
    worker.join().unwrap().ok();

    println!("burst wakeup max latency: {:?}", max_latency);
    assert!(max_latency < LATENCY_LIMIT);
}
