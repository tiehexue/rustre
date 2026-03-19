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

// Windows implementation using TransmitFile API
#[cfg(target_os = "windows")]
use std::os::windows::io::RawSocket;

#[cfg(target_os = "windows")]
use crate::error::RustreError;

#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;

/// Windows zero-copy file transfer using TransmitFile API
///
/// TransmitFile is the Windows equivalent of sendfile() on Unix systems.
/// It transmits file data over a connected socket handle using the Windows
/// kernel's I/O completion ports for efficient, zero-copy data transfer.
#[cfg(target_os = "windows")]
pub fn send_file(
    file: &std::fs::File,
    socket: RawSocket,
    offset: u64,
    length: usize,
) -> Result<usize, RustreError> {
    use std::io;
    use tracing::{debug, error};
    use windows_sys::Win32::Foundation::{FALSE, HANDLE, INVALID_HANDLE_VALUE, TRUE};
    use windows_sys::Win32::Networking::WinSock::{TransmitFile, SOCKET, TF_USE_KERNEL_APC};
    use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};

    let file_handle = file.as_raw_handle() as HANDLE;
    if file_handle == INVALID_HANDLE_VALUE {
        return Err(RustreError::ZeroCopyError("invalid file handle".into()));
    }

    let sock = socket as SOCKET;

    // Set up OVERLAPPED structure for the offset
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    overlapped.Anonymous.Anonymous.Offset = (offset & 0xFFFFFFFF) as u32;
    overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;

    // TransmitFile can send up to 2GB at a time, so we may need to loop for very large files
    let mut total_sent: usize = 0;
    let mut remaining = length;

    while remaining > 0 {
        // TransmitFile's nNumberOfBytesToWrite is DWORD (u32), max ~4GB but practical limit is ~2GB
        let chunk_size = std::cmp::min(remaining, 0x7FFFFFFF) as u32; // 2GB - 1

        // Update overlapped offset for current position
        let current_offset = offset + total_sent as u64;
        overlapped.Anonymous.Anonymous.Offset = (current_offset & 0xFFFFFFFF) as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (current_offset >> 32) as u32;

        let result = unsafe {
            TransmitFile(
                sock,
                file_handle,
                chunk_size,        // nNumberOfBytesToWrite
                0,                 // nNumberOfBytesPerSend (0 = use default)
                &mut overlapped,   // lpOverlapped
                std::ptr::null(),  // lpTransmitBuffers (no header/trailer)
                TF_USE_KERNEL_APC, // dwReserved flags
            )
        };

        if result == 0 {
            let err = io::Error::last_os_error();
            let err_code = err.raw_os_error().unwrap_or(0);

            // Check for ERROR_IO_PENDING (997) - this means async operation is in progress
            if err_code == 997 {
                // ERROR_IO_PENDING - wait for the operation to complete
                let mut bytes_transferred: u32 = 0;
                let wait_result = unsafe {
                    GetOverlappedResult(
                        sock as HANDLE,
                        &overlapped,
                        &mut bytes_transferred,
                        TRUE, // bWait = TRUE, wait for completion
                    )
                };

                if wait_result == FALSE {
                    let wait_err = io::Error::last_os_error();
                    error!(
                        "send_file: GetOverlappedResult failed after IO_PENDING at offset {} with error: {}",
                        current_offset, wait_err
                    );
                    return Err(RustreError::ZeroCopyError(format!(
                        "GetOverlappedResult error: {}",
                        wait_err
                    )));
                }

                // Successfully completed the async operation
                if bytes_transferred as usize != chunk_size as usize {
                    error!(
                        "send_file: partial transfer after IO_PENDING: sent {}/{} bytes at offset {}",
                        bytes_transferred, chunk_size, current_offset
                    );
                    return Err(RustreError::ZeroCopyError(format!(
                        "partial transfer after IO_PENDING: sent {}/{} bytes",
                        bytes_transferred, chunk_size
                    )));
                }

                total_sent += bytes_transferred as usize;
                remaining -= bytes_transferred as usize;
                continue;
            }

            error!(
                "send_file: TransmitFile failed at offset {} with error: {}",
                current_offset, err
            );
            return Err(RustreError::ZeroCopyError(format!(
                "TransmitFile error: {}",
                err
            )));
        }

        // TransmitFile succeeded immediately - it sends the entire requested amount on success
        total_sent += chunk_size as usize;
        remaining -= chunk_size as usize;
    }

    debug!(
        "send_file: sent {} bytes from offset {} to socket (Windows TransmitFile)",
        total_sent, offset
    );

    Ok(total_sent)
}
