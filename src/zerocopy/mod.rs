#[cfg(target_os = "macos")]
use std::os::fd::RawFd;

#[cfg(target_os = "macos")]
use crate::error::RustreError;

#[cfg(target_os = "macos")]
pub fn send_file(
    file_fd: RawFd,
    socket_fd: RawFd,
    offset: u64,
    length: usize,
) -> Result<usize, RustreError> {
    use std::io;
    use tracing::{debug, error};

    // Save current socket flags and set to blocking mode for sendfile
    let flags = unsafe { libc::fcntl(socket_fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(RustreError::ZeroCopyError(
            "failed to get socket flags".into(),
        ));
    }

    // Clear O_NONBLOCK flag
    if unsafe { libc::fcntl(socket_fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) } < 0 {
        return Err(RustreError::ZeroCopyError(
            "failed to set socket to blocking mode".into(),
        ));
    }

    // Convert offset to off_t (platform-specific size)
    let mut off = offset as libc::off_t;

    // Loop until all bytes are sent or error occurs
    let mut total_sent = 0;
    let mut remaining = length;

    while remaining > 0 {
        #[cfg(target_os = "linux")]
        let result =
            unsafe { libc::sendfile(socket_fd, file_fd, &mut off as *mut libc::off_t, remaining) };

        #[cfg(target_os = "macos")]
        let result = unsafe {
            let mut bytes_sent: libc::off_t = length as libc::off_t;
            let ret = libc::sendfile(
                file_fd,
                socket_fd,
                off,
                &mut bytes_sent,
                std::ptr::null_mut(),
                0,
            );

            if ret == 0 {
                bytes_sent as libc::ssize_t
            } else {
                -1
            }
        };

        if result < 0 {
            error!(
                "send_file: sent bytes from offset {} to socket failed: {}",
                offset, result
            );
            let err = io::Error::last_os_error();
            // Restore socket flags before returning error
            let _ = unsafe { libc::fcntl(socket_fd, libc::F_SETFL, flags) };
            return Err(RustreError::ZeroCopyError(format!("I/O error: {}", err)));
        }

        let sent = result as usize;
        if sent == 0 {
            // EOF reached before sending all requested bytes
            // Restore socket flags before returning error
            let _ = unsafe { libc::fcntl(socket_fd, libc::F_SETFL, flags) };
            return Err(RustreError::ZeroCopyError("Partial transfer".into()));
        }

        total_sent += sent;
        remaining -= sent;

        // Update offset for next iteration (Linux updates it automatically)
        #[cfg(target_os = "macos")]
        {
            off += sent as libc::off_t;
        }
    }

    debug!(
        "send_file: sent {} bytes from offset {} to socket",
        total_sent, offset
    );

    // Restore original socket flags
    if unsafe { libc::fcntl(socket_fd, libc::F_SETFL, flags) } < 0 {
        // Log warning but don't fail since sendfile succeeded
        eprintln!("Warning: failed to restore socket flags after sendfile");
    }

    Ok(total_sent)
}
