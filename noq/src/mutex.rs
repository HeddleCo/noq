use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

#[cfg(feature = "lock_tracking")]
mod tracking {
    use super::*;
    use crate::{Duration, Instant};
    use std::collections::VecDeque;
    use tracing::warn;

    #[derive(Debug)]
    struct Inner<T> {
        last_lock_owner: VecDeque<(&'static str, Duration)>,
        value: T,
    }

    /// A Mutex which optionally allows to track the time a lock was held and
    /// emit warnings in case of excessive lock times
    pub(crate) struct Mutex<T> {
        inner: std::sync::Mutex<Inner<T>>,
    }

    impl<T: Debug> Debug for Mutex<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(&self.inner, f)
        }
    }

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self {
                inner: std::sync::Mutex::new(Inner {
                    last_lock_owner: VecDeque::new(),
                    value,
                }),
            }
        }

        /// Acquires the lock for a certain purpose
        ///
        /// The purpose will be recorded in the list of last lock owners
        pub(crate) fn lock(&self, purpose: &'static str) -> MutexGuard<'_, T> {
            // We don't bother dispatching through Runtime::now because they're pure performance
            // diagnostics.
            let now = Instant::now();
            // Poison-tolerant: never panic on a poisoned lock. A previous panic while the
            // connection state was held (e.g. a `bytes` invariant violation on a mid-flight
            // teardown) would otherwise poison this mutex, and every subsequent `.lock()` —
            // including those reached from `Drop` impls during unwinding — would panic. A
            // panic in a `Drop` during unwind aborts the whole process. Recovering the guard
            // via `into_inner()` converts that fatal, process-wide abort into a recoverable
            // per-connection failure. See weft#2077.
            let guard = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            let lock_time = Instant::now();
            let elapsed = lock_time.duration_since(now);

            if elapsed > Duration::from_millis(1) {
                warn!(
                    "Locking the connection for {} took {:?}. Last owners: {:?}",
                    purpose, elapsed, guard.last_lock_owner
                );
            }

            MutexGuard {
                guard,
                start_time: lock_time,
                purpose,
            }
        }
    }

    pub(crate) struct MutexGuard<'a, T> {
        guard: std::sync::MutexGuard<'a, Inner<T>>,
        start_time: Instant,
        purpose: &'static str,
    }

    impl<T> Drop for MutexGuard<'_, T> {
        fn drop(&mut self) {
            if self.guard.last_lock_owner.len() == MAX_LOCK_OWNERS {
                self.guard.last_lock_owner.pop_back();
            }

            let duration = self.start_time.elapsed();

            if duration > Duration::from_millis(1) {
                warn!(
                    "Utilizing the connection for {} took {:?}",
                    self.purpose, duration
                );
            }

            self.guard
                .last_lock_owner
                .push_front((self.purpose, duration));
        }
    }

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.guard.value
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.guard.value
        }
    }

    const MAX_LOCK_OWNERS: usize = 20;
}

#[cfg(feature = "lock_tracking")]
pub(crate) use tracking::{Mutex, MutexGuard};

#[cfg(not(feature = "lock_tracking"))]
mod non_tracking {
    use super::*;

    /// A Mutex which optionally allows to track the time a lock was held and
    /// emit warnings in case of excessive lock times
    #[derive(Debug)]
    pub(crate) struct Mutex<T> {
        inner: std::sync::Mutex<T>,
    }

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self {
                inner: std::sync::Mutex::new(value),
            }
        }

        /// Acquires the lock for a certain purpose
        ///
        /// The purpose will be recorded in the list of last lock owners
        pub(crate) fn lock(&self, _purpose: &'static str) -> MutexGuard<'_, T> {
            MutexGuard {
                // Poison-tolerant: never panic on a poisoned lock. A previous panic while the
                // connection state was held (e.g. a `bytes` invariant violation on a mid-flight
                // teardown) would otherwise poison this mutex, and every subsequent `.lock()` —
                // including those reached from `Drop` impls during unwinding — would panic. A
                // panic in a `Drop` during unwind aborts the whole process. Recovering the guard
                // via `into_inner()` converts that fatal, process-wide abort into a recoverable
                // per-connection failure. See weft#2077.
                guard: self
                    .inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            }
        }
    }

    pub(crate) struct MutexGuard<'a, T> {
        guard: std::sync::MutexGuard<'a, T>,
    }

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            self.guard.deref()
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.guard.deref_mut()
        }
    }
}

#[cfg(not(feature = "lock_tracking"))]
pub(crate) use non_tracking::{Mutex, MutexGuard};

#[cfg(test)]
mod poison_tests {
    use super::Mutex;
    use std::sync::Arc;

    /// Regression test for weft#2077: acquiring a poisoned lock must not panic.
    ///
    /// Before the fix, `lock()` did `.unwrap()` on the result, so a lock that had
    /// been poisoned by a panic (while the connection state was held) would panic
    /// on every subsequent acquisition. Because such acquisitions happen inside
    /// `Drop` impls during unwinding, that second panic aborted the whole process.
    #[test]
    fn lock_is_poison_tolerant() {
        let mutex = Arc::new(Mutex::new(0u32));

        // Poison the inner std mutex by panicking while the guard is held.
        let poisoner = {
            let mutex = mutex.clone();
            std::thread::spawn(move || {
                let mut guard = mutex.lock("poison");
                *guard = 41;
                panic!("intentional panic to poison the mutex");
            })
        };
        assert!(poisoner.join().is_err(), "poisoner thread should panic");

        // Acquiring the lock again must NOT panic despite the poison, and must
        // recover the guarded value.
        let mut guard = mutex.lock("recover");
        assert_eq!(*guard, 41, "poisoned data should be recovered, not lost");
        *guard += 1;
        assert_eq!(*guard, 42);
    }

    /// Regression test for the actual abort vector of weft#2077.
    ///
    /// A poisoned lock is acquired from a `Drop` impl that runs *during unwinding*
    /// of another panic. Before the fix the `.unwrap()` panicked there, and a panic
    /// while already unwinding is a non-unwinding double panic that calls
    /// `abort()`, killing the whole process (and this test binary). After the fix
    /// the lock is recovered without panicking and unwinding completes normally.
    #[test]
    fn poisoned_lock_in_drop_during_unwind_does_not_abort() {
        struct LocksOnDrop(Arc<Mutex<u32>>);
        impl Drop for LocksOnDrop {
            fn drop(&mut self) {
                // Runs while the panic below is unwinding the stack.
                let _guard = self.0.lock("drop-during-unwind");
            }
        }

        let mutex = Arc::new(Mutex::new(0u32));

        // Poison the mutex.
        let poisoner = {
            let mutex = mutex.clone();
            std::thread::spawn(move || {
                let _guard = mutex.lock("poison");
                panic!("intentional panic to poison the mutex");
            })
        };
        assert!(poisoner.join().is_err());

        // Panic with a `LocksOnDrop` on the stack so its `Drop` acquires the
        // poisoned lock during unwinding. `catch_unwind` keeps the test alive if
        // (and only if) that `Drop` did not itself panic.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _drops_and_locks = LocksOnDrop(mutex.clone());
            panic!("trigger unwind with a poisoned-lock Drop on the stack");
        }));
        assert!(result.is_err(), "the induced panic should have been caught");
        // Reaching here means the Drop did not double-panic/abort.
    }
}
