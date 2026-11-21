//! A managed pool of worker threads with automatic blocking-task isolation.
//!
//! The pool keeps a fixed number of async workers driving an [`Executor`]. A
//! watchdog thread observes how long each worker spends inside a single poll.
//! When a poll runs past [`BLOCKING_THRESHOLD`], the task running inside it is
//! classified as blocking without any cooperation from the caller:
//!
//! 1. The task's metadata is flagged, so every later reschedule lands on the
//!    dedicated blocking queue instead of the async queue.
//! 2. The stuck worker is asked to exit once its current poll returns, and a
//!    replacement worker is spawned immediately, keeping async concurrency at
//!    the configured level while the blocked thread keeps running.
//! 3. Blocking runnables are executed by separate, lazily spawned blocking
//!    threads, so they can never starve async work.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::future::Future;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::thread::{self, available_parallelism, JoinHandle, Thread};
use std::time::{Duration, Instant};

use crate::{Executor, WorkerSlot, CURRENT_SLOT, MAX_BLOCKING_THREADS};

/// Minimal single-future executor used on worker threads.
///
/// The crate's own `futures-lite` dependency is built without its `std`
/// feature, so its `block_on` is unavailable here. This implementation parks
/// the worker thread until the future's waker wakes it again. The `on_park`
/// callback runs before every park so the loop can bail out on shutdown even
/// while the future has no waker registered yet.
fn block_on_with<F: Future>(
    mut future: F,
    mut on_park: impl FnMut() -> bool,
) -> Option<F::Output> {
    // SAFETY: The future stays on this stack frame and is never moved again.
    let mut future = unsafe { Pin::new_unchecked(&mut future) };

    let waker = std::task::Waker::from(Arc::new(BlockOnWaker {
        thread: thread::current(),
    }));
    let mut cx = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Some(output);
        }
        if on_park() {
            return None;
        }
        // `park_timeout` is a belt-and-suspenders wait: the waker unparks the
        // worker immediately when a task becomes ready, while the timeout
        // guarantees progress even if a spurious/lost unpark token was consumed
        // elsewhere inside the executor's scheduling machinery.
        thread::park_timeout(PARK_TIMEOUT);
    }
}

/// One-token waker that simply unparks the worker thread.
struct BlockOnWaker {
    thread: Thread,
}

impl std::task::Wake for BlockOnWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }
}

/// How often the watchdog inspects the async workers and blocking demand.
const WATCHDOG_INTERVAL: Duration = Duration::from_millis(2);

/// Fallback park timeout for worker threads waiting for the next task.
const PARK_TIMEOUT: Duration = Duration::from_millis(5);

/// How long an idle blocking thread waits for work before exiting.
const BLOCKING_IDLE_TIMEOUT: Duration = Duration::from_millis(200);

/// How many extra workers may be spawned to cover blocked ones.
///
/// The pool never runs more than `worker_count` healthy async workers at once;
/// this only bounds the number of blocked workers waiting to exit.
const MAX_REPLACEMENTS: usize = 1024;

/// Signals pool shutdown to the worker loops.
///
/// Workers observe the flag directly (before each park and on every park
/// timeout), so no waker needs to be stored here.
struct Shutdown {
    notified: AtomicBool,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            notified: AtomicBool::new(false),
        }
    }

    fn trigger(&self) {
        self.notified.store(true, Ordering::Release);
    }
}

/// Shared state behind every [`WorkerPool`].
struct PoolState {
    /// The executor driven by the pool.
    executor: Arc<Executor<'static>>,

    /// Number of healthy async workers the pool maintains.
    worker_count: usize,

    /// Slots of all async workers, including blocked ones awaiting exit.
    slots: Mutex<Vec<Arc<WorkerSlot>>>,

    /// Async worker join handles.
    workers: Mutex<Vec<WorkerHandle>>,

    /// Blocking thread join handles.
    blocking: Mutex<Vec<JoinHandle<()>>>,

    /// Replacement workers spawned to cover blocked workers.
    replacements: AtomicUsize,

    /// Total number of tasks classified as blocking.
    blocked_tasks: AtomicUsize,

    /// Set when the pool is dropped.
    shutdown: Shutdown,
}

/// An async worker thread together with its monitored slot.
struct WorkerHandle {
    slot: Arc<WorkerSlot>,
    handle: JoinHandle<()>,
}

/// A thread pool that drives an [`Executor`] and isolates blocking tasks.
///
/// The pool spawns one async worker per logical CPU by default (see
/// [`WorkerPool::new`]) and keeps that level of concurrency even when spawned
/// tasks perform synchronous blocking operations. Blocking is detected
/// automatically from observed poll durations; tasks do not need to be marked
/// or wrapped by the caller.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use async_executor::{Executor, WorkerPool};
///
/// let executor = Arc::new(Executor::new());
/// let pool = WorkerPool::new(executor.clone());
///
/// let task = executor.spawn(async move {
///     // A synchronous blocking call: detected and isolated automatically.
///     std::thread::sleep(Duration::from_millis(50));
///     42
/// });
///
/// assert_eq!(futures_lite::future::block_on(task), 42);
/// drop(pool);
/// ```
pub struct WorkerPool {
    state: Arc<PoolState>,

    /// Watchdog handle, joined on shutdown.
    watchdog: Option<JoinHandle<()>>,
}

impl fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerPool")
            .field("worker_count", &self.worker_count())
            .field("blocked_tasks", &self.blocked_task_count())
            .finish()
    }
}

impl WorkerPool {
    /// Creates a pool with one async worker per logical CPU.
    ///
    /// The worker count comes from [`available_parallelism`] and is always at
    /// least one.
    #[must_use]
    pub fn new(executor: Arc<Executor<'static>>) -> Self {
        let worker_count = available_parallelism().map(|n| n.get()).unwrap_or(1).max(1);
        Self::with_worker_count(executor, worker_count)
    }

    /// Creates a pool with an explicit number of async workers.
    ///
    /// `worker_count` is clamped to at least one.
    #[must_use]
    pub fn with_worker_count(executor: Arc<Executor<'static>>, worker_count: usize) -> Self {
        let worker_count = worker_count.max(1);
        let state = Arc::new(PoolState {
            executor,
            worker_count,
            slots: Mutex::new(Vec::new()),
            workers: Mutex::new(Vec::new()),
            blocking: Mutex::new(Vec::new()),
            replacements: AtomicUsize::new(0),
            blocked_tasks: AtomicUsize::new(0),
            shutdown: Shutdown::new(),
        });

        for _ in 0..worker_count {
            state.spawn_worker();
        }

        let watchdog_state = state.clone();
        let watchdog = thread::Builder::new()
            .name("async-executor-watchdog".to_owned())
            .spawn(move || watchdog_state.run_watchdog())
            .expect("failed to spawn blocking watchdog thread");

        Self {
            state,
            watchdog: Some(watchdog),
        }
    }

    /// Returns the number of healthy async workers the pool maintains.
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.state.worker_count
    }

    /// Returns the number of tasks that have been classified as blocking.
    #[must_use]
    pub fn blocked_task_count(&self) -> usize {
        self.state.blocked_tasks.load(Ordering::Acquire)
    }

    /// Returns the number of runnables executed on dedicated blocking threads.
    ///
    /// A non-zero value proves that blocking work is genuinely isolated from
    /// the async workers rather than merely being covered by extra threads.
    #[must_use]
    pub fn isolated_run_count(&self) -> usize {
        self.state
            .executor
            .state_blocking()
            .processed_count()
    }
}

impl PoolState {
    /// Spawns one async worker and registers its slot.
    fn spawn_worker(self: &Arc<Self>) {
        let slot = WorkerSlot::new();
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(slot.clone());

        let state = self.clone();
        let worker_slot = slot.clone();
        let handle = thread::Builder::new()
            .name("async-executor-worker".to_owned())
            .spawn(move || run_worker(state, slot.clone()))
            .expect("failed to spawn async worker thread");
        self.workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(WorkerHandle {
                slot: worker_slot,
                handle,
            });
    }

    /// Watchdog loop: detects blocked workers, isolates their tasks, replaces
    /// blocked workers and grows the blocking pool on demand.
    fn run_watchdog(self: Arc<Self>) {
        let blocking = self.executor.state_blocking();
        let mut last_enqueued = 0usize;

        while !self.shutdown.notified.load(Ordering::Acquire) {
            thread::sleep(WATCHDOG_INTERVAL);

            self.detect_blocked_workers(Instant::now());
            last_enqueued = self.grow_blocking_pool(blocking, last_enqueued);
        }
    }

    /// Marks any worker whose current poll exceeds the threshold as blocked and
    /// spawns a replacement so the configured async concurrency is preserved.
    fn detect_blocked_workers(self: &Arc<Self>, now: Instant) {
        let stuck: Vec<Arc<WorkerSlot>> = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|slot| slot.is_stuck(now))
            .cloned()
            .collect();

        for slot in stuck {
            // Idempotent: a still-blocked worker is only handled once.
            if slot.is_evicted() {
                continue;
            }
            slot.evict();
            self.blocked_tasks.fetch_add(1, Ordering::AcqRel);

            if self.replacements.fetch_add(1, Ordering::AcqRel) < MAX_REPLACEMENTS {
                self.spawn_worker();
            } else {
                self.replacements.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Spawns blocking threads while queued demand outruns live blocking threads.
    fn grow_blocking_pool(self: &Arc<Self>, blocking: &crate::BlockingQueue, last: usize) -> usize {
        let enqueued = blocking.enqueued_count();
        if enqueued == last {
            return last;
        }

        // Keep at least one live blocking thread per queued runnable, up to the
        // cap. Threads busy with earlier runnables are not idle, but `len()`
        // only counts items nobody has popped yet, so this grows exactly when
        // queued work outpaces the current blocking fleet.
        while blocking.thread_count() < MAX_BLOCKING_THREADS
            && blocking.thread_count() <= blocking.len()
        {
            self.spawn_blocking_thread(blocking);
        }
        enqueued
    }

    /// Spawns a single blocking thread.
    fn spawn_blocking_thread(self: &Arc<Self>, blocking: &crate::BlockingQueue) {
        if blocking.thread_count() >= MAX_BLOCKING_THREADS {
            return;
        }
        blocking.add_thread();

        let state = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("async-executor-blocking".to_owned())
            .spawn(move || run_blocking(state))
            .expect("failed to spawn blocking thread");
        self.blocking
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(handle);
    }
}

/// Async worker loop.
fn run_worker(state: Arc<PoolState>, slot: Arc<WorkerSlot>) {
    CURRENT_SLOT.with(|cell| cell.set(Some(Arc::as_ptr(&slot))));

    loop {
        if state.shutdown.notified.load(Ordering::Acquire) {
            break;
        }

        // Drive exactly one tick. Only the tick's own waker gets registered,
        // so shutdown bookkeeping can never clobber it; the `on_park` callback
        // breaks out of the park loop when the pool shuts down.
        let tick = state.executor.tick();
        let shutdown = &state.shutdown;
        let result = catch_unwind(AssertUnwindSafe(|| {
            block_on_with(tick, || shutdown.notified.load(Ordering::Acquire))
        }));
        drop(result);

        // A task panic is propagated through its handle by `async-task`; the
        // worker stays alive unless the watchdog evicted it for blocking.
        if slot.is_evicted() {
            break;
        }
        if state.shutdown.notified.load(Ordering::Acquire) {
            break;
        }
    }

    CURRENT_SLOT.with(|cell| cell.set(None));
    // Reached only once the current poll returns. A worker blocked inside a
    // long poll does not get here until that poll ends, so `Drop` uses this
    // flag to distinguish joinable workers from ones it must detach.
    slot.mark_finished();
}

/// Blocking thread loop. Exits after being idle for a while or on shutdown.
fn run_blocking(state: Arc<PoolState>) {
    let blocking = state.executor.state_blocking();

    loop {
        match blocking.pop_timeout(BLOCKING_IDLE_TIMEOUT) {
            Some(runnable) => {
                blocking.record_processed();
                // Blocking runnables never register a worker slot, so a long
                // poll here cannot be misclassified again.
                let _ = catch_unwind(AssertUnwindSafe(|| runnable.run()));
            }
            None => {
                if state.shutdown.notified.load(Ordering::Acquire)
                    || blocking.is_shutdown()
                {
                    break;
                }
                // Timed out with no work: retire this thread.
                break;
            }
        }
    }

    blocking.remove_thread();
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Stop the async loops and the watchdog.
        self.state.shutdown.trigger();
        if let Some(handle) = self.watchdog.take() {
            let _ = handle.join();
        }

        // Wake blocking threads so they observe shutdown and exit. Threads
        // currently running a blocking task are intentionally *not* joined:
        // blocking work may legitimately take arbitrarily long, and the pool
        // must be droppable regardless. They detach and retire themselves once
        // their current task finishes (or immediately if they were idle).
        let blocking = self.state.executor.state_blocking();
        blocking.shutdown();

        // Idle workers observe the flag and exit within one park timeout. Wait
        // for that window so their `finished` flags are visible before we
        // decide which threads can be joined.
        thread::sleep(PARK_TIMEOUT + Duration::from_millis(5));

        // Join workers whose run loops have fully exited. Workers still inside
        // a blocking poll have not finished and cannot be joined without
        // waiting for the task, so detach them; they retire once the poll
        // returns and keep the executor alive through their `Arc<PoolState>`.
        let worker_handles = core::mem::take(
            &mut *self.state.workers.lock().unwrap_or_else(PoisonError::into_inner),
        );
        for worker in worker_handles {
            if worker.slot.is_finished() {
                let _ = worker.handle.join();
            } else {
                // Detach: drop the join handle but keep the thread running.
                drop(worker.handle);
            }
        }

        // Drop the blocking join handles without joining: detaching the
        // threads. The `BlockingQueue` shutdown flag guarantees they cannot
        // pick up new work and will wind down on their own.
        let _handles = core::mem::take(
            &mut *self.state.blocking.lock().unwrap_or_else(PoisonError::into_inner),
        );
    }
}
