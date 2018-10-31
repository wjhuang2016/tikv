use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use libc::{getpid, pid_t};

use super::server::GRPC_THREAD_PREFIX;
use util::metrics::{get_thread_ids, Stat};

// TODO: make it more pretty.
// pub struct Load {
//     term_and_load: Arc<(AtomicUsize, AtomicUsize)>,
//     heavy_load_threshold: usize,
// }

#[cfg(target_os = "linux")]
pub(super) struct GrpcThreadLoadStatistics {
    pid: pid_t,
    tids: Vec<pid_t>,
    capacity: usize,
    cur_pos: usize,
    cpu_usages: Vec<f64>,
    instants: Vec<Instant>,
    in_heavy_load: Arc<(AtomicUsize, AtomicUsize)>,
}

#[cfg(target_os = "linux")]
impl GrpcThreadLoadStatistics {
    pub(super) fn new(capacity: usize, in_heavy_load: Arc<(AtomicUsize, AtomicUsize)>) -> Self {
        let pid: pid_t = unsafe { getpid() };
        let mut tids = vec![];
        let mut cpu_total = 0f64;
        for tid in get_thread_ids(pid).unwrap() {
            if let Ok(stat) = Stat::collect(pid, tid) {
                if !stat.name().starts_with(GRPC_THREAD_PREFIX) {
                    continue;
                }
                cpu_total += stat.cpu_total();
                tids.push(tid);
            }
        }
        GrpcThreadLoadStatistics {
            pid,
            tids,
            capacity,
            cur_pos: 0,
            cpu_usages: vec![cpu_total; capacity],
            instants: vec![Instant::now(); capacity],
            in_heavy_load,
        }
    }

    pub(super) fn record(&mut self, instant: Instant) {
        self.instants[self.cur_pos] = instant;
        self.cpu_usages[self.cur_pos] = 0f64;
        for tid in &self.tids {
            let stat = Stat::collect(self.pid, *tid).unwrap();
            self.cpu_usages[self.cur_pos] += stat.cpu_total();
        }
        let current_instant = self.instants[self.cur_pos];
        let current_cpu_usage = self.cpu_usages[self.cur_pos];

        let next_pos = (self.cur_pos + 1) % self.capacity;
        let earlist_instant = self.instants[next_pos];
        let earlist_cpu_usage = self.cpu_usages[next_pos];
        self.cur_pos = next_pos;

        let millis = (current_instant - earlist_instant).as_millis() as usize;
        if millis > 0 {
            let cpu_usage = (current_cpu_usage - earlist_cpu_usage) * 1000f64 * 100f64;
            let cpu_usage = cpu_usage as usize / millis;
            self.in_heavy_load.1.store(cpu_usage, Ordering::SeqCst);
            self.in_heavy_load.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}
