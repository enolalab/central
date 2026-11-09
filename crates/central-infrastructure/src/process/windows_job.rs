//! Windows Job Object ownership for agent processes (P2 Task 2).
//!
//! Owned agent processes are assigned to a Job Object created with
//! `KILL_ON_JOB_CLOSE`, so the whole process tree dies with Central and
//! nothing outlives the supervisor. Enforcement is query-backed: after
//! setting the limit flag it is read back, and [`JobObject`] construction
//! fails unless the flag is observably set. When a job cannot be enforced
//! (for example a nested job inside a sandbox that forbids nesting), the
//! supervisor publishes [`JobPlacement::InstructionOnly`] instead of
//! pretending.
//!
//! Termination never kills by PID alone: [`process_creation_time_ms`] lets
//! the supervisor refuse a stale identity whose PID may have been recycled.

use std::fmt;

#[cfg(windows)]
use std::os::raw::{c_int, c_void};

/// How kill-on-close containment was established for supervised processes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobPlacement {
    /// A job object with KILL_ON_JOB_CLOSE was created and the flag was
    /// read back as set.
    Enforced,
    /// Containment is unenforceable here; processes are supervised by
    /// handle only. The reason is published, never silent.
    InstructionOnly(String),
}

/// Job object failures. Opening or querying a dead PID is `NotFound` so the
/// supervisor fails closed (kills nothing) instead of guessing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobError {
    Win32(u32),
    NotFound(String),
    Unsupported(String),
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Win32(code) => write!(f, "windows error {code}"),
            Self::NotFound(message) => write!(f, "process not found: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported: {message}"),
        }
    }
}

impl std::error::Error for JobError {}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::ptr::null;

    type Handle = *mut c_void;
    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: c_int = 9;
    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;
    const PROCESS_SET_QUOTA: u32 = 0x0100;
    const PROCESS_TERMINATE_ACCESS: u32 = 0x0001;
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    /// 100ns ticks between 1601-01-01 and 1970-01-01.
    const FILETIME_TO_UNIX_EPOCH: i64 = 116_444_736_000_000_000;

    #[repr(C)]
    struct JobExtendedLimitInformation {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: u32,
        _pad0: u32,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: u32,
        _pad1: u32,
        affinity: usize,
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateJobObjectW(attributes: *const c_void, name: *const u16) -> Handle;
        fn SetInformationJobObject(
            job: Handle,
            class: c_int,
            info: *const c_void,
            len: u32,
        ) -> c_int;
        fn QueryInformationJobObject(
            job: Handle,
            class: c_int,
            info: *mut c_void,
            len: u32,
            ret_len: *mut u32,
        ) -> c_int;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> c_int;
        fn OpenProcess(desired_access: u32, inherit_handle: c_int, pid: u32) -> Handle;
        fn CloseHandle(handle: Handle) -> c_int;
        fn GetProcessTimes(
            process: Handle,
            creation: *mut u64,
            exit: *mut u64,
            kernel: *mut u64,
            user: *mut u64,
        ) -> c_int;
        fn GetExitCodeProcess(process: Handle, code: *mut u32) -> c_int;
        fn IsProcessInJob(process: Handle, job: Handle, result: *mut c_int) -> c_int;
        fn GetCurrentProcess() -> Handle;
        fn GetLastError() -> u32;
    }

    fn last_error() -> JobError {
        // SAFETY: GetLastError has no preconditions.
        JobError::Win32(unsafe { GetLastError() })
    }

    fn close(handle: Handle) {
        // SAFETY: handle came from a successful kernel32 open/create call.
        unsafe {
            CloseHandle(handle);
        }
    }

    /// A job object with KILL_ON_JOB_CLOSE observably set (read back after
    /// writing). Closing the last handle kills every assigned process.
    pub struct JobObject {
        handle: Handle,
    }

    // The handle is an opaque kernel object reference; all methods take
    // `&self` and every call is atomic from the kernel side.
    unsafe impl Send for JobObject {}

    impl Drop for JobObject {
        fn drop(&mut self) {
            close(self.handle);
        }
    }

    impl JobObject {
        pub fn new_kill_on_close() -> Result<Self, JobError> {
            // SAFETY: null attributes/name create an unnamed private job.
            let handle = unsafe { CreateJobObjectW(null(), null()) };
            if handle.is_null() {
                return Err(last_error());
            }
            // SAFETY: zeroed struct with only limit_flags set is valid input.
            let mut info: JobExtendedLimitInformation = unsafe { std::mem::zeroed() };
            info.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: info is a valid JobExtendedLimitInformation of the given size.
            let ok = unsafe {
                SetInformationJobObject(
                    handle,
                    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                    &info as *const _ as *const c_void,
                    std::mem::size_of::<JobExtendedLimitInformation>() as u32,
                )
            };
            if ok == 0 {
                let error = last_error();
                close(handle);
                return Err(error);
            }
            // Query-backed proof: the flag must read back as set, otherwise
            // enforcement is a claim without evidence.
            // SAFETY: zeroed output buffer of the documented size.
            let mut back: JobExtendedLimitInformation = unsafe { std::mem::zeroed() };
            let mut ret_len: u32 = 0;
            // SAFETY: back is a valid output buffer of the given size.
            let ok = unsafe {
                QueryInformationJobObject(
                    handle,
                    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                    &mut back as *mut _ as *mut c_void,
                    std::mem::size_of::<JobExtendedLimitInformation>() as u32,
                    &mut ret_len,
                )
            };
            if ok == 0 || (back.limit_flags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE) == 0 {
                let error = if ok == 0 {
                    last_error()
                } else {
                    JobError::Win32(0)
                };
                close(handle);
                return Err(error);
            }
            Ok(Self { handle })
        }

        /// Assign a live PID to this job. Fails when the process is already
        /// in a job (nested jobs are forbidden without an explicit allowance
        /// on the parent job): the caller must publish that, not ignore it.
        pub fn assign(&self, pid: u32) -> Result<(), JobError> {
            // SAFETY: numeric PID with no handle inheritance.
            let process =
                unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE_ACCESS, 0, pid) };
            if process.is_null() {
                return Err(JobError::NotFound(format!(
                    "cannot open pid {pid} for job assignment"
                )));
            }
            // SAFETY: both handles are live kernel handles.
            let ok = unsafe { AssignProcessToJobObject(self.handle, process) };
            close(process);
            if ok == 0 {
                return Err(last_error());
            }
            Ok(())
        }
    }

    /// Creation time of a live PID as Unix milliseconds. Fails closed with
    /// `NotFound` when the PID cannot be opened (dead or recycled PID must
    /// never be mistaken for the supervised process).
    pub fn process_creation_time_ms(pid: u32) -> Result<i64, JobError> {
        // SAFETY: numeric PID with no handle inheritance.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if process.is_null() {
            return Err(JobError::NotFound(format!("cannot open pid {pid}")));
        }
        let mut creation: u64 = 0;
        let mut exit: u64 = 0;
        let mut kernel: u64 = 0;
        let mut user: u64 = 0;
        // SAFETY: all out-pointers are valid u64 slots.
        let ok =
            unsafe { GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) };
        close(process);
        if ok == 0 {
            return Err(JobError::NotFound(format!(
                "cannot read creation time of pid {pid}"
            )));
        }
        Ok((creation as i64 - FILETIME_TO_UNIX_EPOCH) / 10_000)
    }

    /// Exit code of a live PID, or `None` while it is still running.
    pub fn process_exit_code(pid: u32) -> Result<Option<i32>, JobError> {
        // SAFETY: numeric PID with no handle inheritance.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if process.is_null() {
            return Err(JobError::NotFound(format!("cannot open pid {pid}")));
        }
        let mut code: u32 = 0;
        // SAFETY: code is a valid u32 slot.
        let ok = unsafe { GetExitCodeProcess(process, &mut code) };
        close(process);
        if ok == 0 {
            return Err(last_error());
        }
        if code == STILL_ACTIVE {
            Ok(None)
        } else {
            Ok(Some(code as i32))
        }
    }

    fn process_in_job_handle(process: Handle) -> Result<bool, JobError> {
        let mut result: c_int = 0;
        // SAFETY: result is a valid out-slot; null job queries any job.
        let ok = unsafe { IsProcessInJob(process, null::<c_void>() as Handle, &mut result) };
        if ok == 0 {
            return Err(last_error());
        }
        Ok(result != 0)
    }

    /// Whether a live PID is currently assigned to any job.
    pub fn process_in_job(pid: u32) -> Result<bool, JobError> {
        // SAFETY: numeric PID with no handle inheritance.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if process.is_null() {
            return Err(JobError::NotFound(format!("cannot open pid {pid}")));
        }
        let answer = process_in_job_handle(process);
        close(process);
        answer
    }

    /// Whether this process is currently assigned to any job (for example a
    /// sandbox or CLI-tied job). Used to pick honest placement reporting.
    pub fn current_process_in_job() -> Result<bool, JobError> {
        // SAFETY: GetCurrentProcess returns a valid pseudo-handle.
        process_in_job_handle(unsafe { GetCurrentProcess() })
    }
}

#[cfg(not(windows))]
mod imp {
    use super::*;

    /// No job objects off Windows: construction always reports why, so the
    /// supervisor publishes `InstructionOnly` instead of pretending.
    pub struct JobObject {
        _private: (),
    }

    impl JobObject {
        pub fn new_kill_on_close() -> Result<Self, JobError> {
            Err(JobError::Unsupported(
                "job objects are Windows-only; contained processes need a Windows host".to_string(),
            ))
        }

        pub fn assign(&self, _pid: u32) -> Result<(), JobError> {
            Err(JobError::Unsupported(
                "job objects are Windows-only".to_string(),
            ))
        }
    }

    pub fn process_creation_time_ms(_pid: u32) -> Result<i64, JobError> {
        Err(JobError::Unsupported(
            "process creation time needs the Windows implementation".to_string(),
        ))
    }

    pub fn process_exit_code(_pid: u32) -> Result<Option<i32>, JobError> {
        Err(JobError::Unsupported(
            "process exit query needs the Windows implementation".to_string(),
        ))
    }

    pub fn process_in_job(_pid: u32) -> Result<bool, JobError> {
        // No job objects exist on this platform, so no process is in one.
        Ok(false)
    }

    pub fn current_process_in_job() -> Result<bool, JobError> {
        Ok(false)
    }
}

pub use imp::{
    current_process_in_job, process_creation_time_ms, process_exit_code, process_in_job, JobObject,
};
