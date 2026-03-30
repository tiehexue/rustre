pub fn get_ugid() -> (u32, u32) {
    #[cfg(unix)]
    let uid = unsafe { libc::getuid() };
    #[cfg(unix)]
    let gid = unsafe { libc::getgid() };

    #[cfg(not(unix))]
    let uid = 0;
    #[cfg(not(unix))]
    let gid = 0;

    (uid, gid)
}
