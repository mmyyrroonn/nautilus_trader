// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Opt-in poll observations; no transport decisions use these clocks.

use std::{
    future::poll_fn,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use futures_util::Stream;

use super::types::WebSocketReadTiming;

/// Reused for the reader task's lifetime; allocates only when the task waker changes.
#[derive(Default)]
pub(super) struct ReadObserver(Option<(Arc<ObservedWake>, Waker)>);

impl ReadObserver {
    pub(super) async fn next<S: Stream + Unpin>(
        &mut self,
        reader: &mut S,
    ) -> (Option<S::Item>, Option<WebSocketReadTiming>) {
        let mut was_pending = false;
        poll_fn(|cx| {
            if self
                .0
                .as_ref()
                .is_none_or(|(wake, _)| !wake.parent.will_wake(cx.waker()))
            {
                let wake = Arc::new(ObservedWake {
                    parent: cx.waker().clone(),
                    origin: Instant::now(), // dst-ok: Diagnostic only; never drives transport state
                    first_wake: AtomicU64::new(0),
                });
                let waker = Waker::from(Arc::clone(&wake));
                self.0 = Some((wake, waker));
            }
            let (wake, waker) = self.0.as_ref().expect("observer initialized");
            let wake_ns = wake.first_wake.swap(0, Ordering::Relaxed);
            let poll_started = Instant::now(); // dst-ok: Diagnostic only; never drives transport state
            let mut observed_cx = Context::from_waker(waker);
            match Pin::new(&mut *reader).poll_next(&mut observed_cx) {
                Poll::Ready(item) => {
                    let completed = Instant::now(); // dst-ok: Diagnostic only; never drives transport state
                    let wake_observed = (was_pending && wake_ns > 0)
                        .then(|| wake.origin + Duration::from_nanos(wake_ns - 1));
                    Poll::Ready((
                        item,
                        Some(WebSocketReadTiming {
                            wake_observed,
                            poll_started,
                            completed,
                        }),
                    ))
                }
                Poll::Pending => {
                    was_pending = true;
                    Poll::Pending
                }
            }
        })
        .await
    }
}

struct ObservedWake {
    parent: Waker,
    origin: Instant,
    first_wake: AtomicU64,
}

impl Wake for ObservedWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let now = Instant::now(); // dst-ok: Diagnostic only; never drives transport state
        let elapsed = u64::try_from(now.duration_since(self.origin).as_nanos())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1);
        let _ = self
            .first_wake
            .compare_exchange(0, elapsed, Ordering::Relaxed, Ordering::Relaxed);
        self.parent.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::pin,
        sync::{Mutex, atomic::AtomicBool},
    };

    use futures_util::{stream, task::noop_waker};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn wake_to_poll_excludes_idle_wait_and_preserves_forwarding() {
        let ready = AtomicBool::new(false);
        let saved = Mutex::new(None::<Waker>);
        let mut reader = stream::poll_fn(|cx| {
            if ready.load(Ordering::Relaxed) {
                Poll::Ready(Some(7))
            } else {
                *saved.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        });
        let parent = Arc::new(CountingWake(AtomicU64::new(0)));
        let waker = Waker::from(Arc::clone(&parent));
        let mut cx = Context::from_waker(&waker);
        let mut observer = ReadObserver::default();
        {
            let mut next = pin!(observer.next(&mut reader));
            assert!(next.as_mut().poll(&mut cx).is_pending());
            std::thread::sleep(Duration::from_millis(20));
            ready.store(true, Ordering::Relaxed);
            let before_wake = Instant::now();
            saved.lock().unwrap().as_ref().unwrap().wake_by_ref();
            std::thread::sleep(Duration::from_millis(15));
            saved.lock().unwrap().as_ref().unwrap().wake_by_ref();
            let Poll::Ready((Some(7), Some(read))) = next.as_mut().poll(&mut cx) else {
                panic!("Expected ready observation");
            };
            assert!(read.wake_observed.unwrap() >= before_wake);
            assert!(
                read.poll_started
                    .duration_since(read.wake_observed.unwrap())
                    >= Duration::from_millis(15)
            );
            assert!(read.completed >= read.poll_started);
            assert_eq!(parent.0.load(Ordering::Relaxed), 2);
        }
        // Buffered messages have no new pending read, so cannot inherit an old wake
        let mut next = pin!(observer.next(&mut reader));
        let Poll::Ready((Some(7), Some(read))) = next.as_mut().poll(&mut cx) else {
            panic!("Expected buffered message");
        };
        assert!(read.wake_observed.is_none());
    }

    #[rstest]
    fn canceled_read_does_not_attribute_a_previous_wake_to_buffered_data() {
        let mut observer = ReadObserver::default();
        let mut reader = stream::pending::<u8>();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        {
            let mut next = pin!(observer.next(&mut reader));
            assert!(next.as_mut().poll(&mut cx).is_pending());
        }
        observer.0.as_ref().unwrap().1.wake_by_ref();
        let mut reader = stream::iter([1]);
        let mut next = pin!(observer.next(&mut reader));
        let Poll::Ready((Some(1), Some(read))) = next.as_mut().poll(&mut cx) else {
            panic!("Expected buffered message");
        };
        assert!(read.wake_observed.is_none());
    }

    struct CountingWake(AtomicU64);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}
