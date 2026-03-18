//! Path utilities for MDS

/// Normalize path: ensure leading /, remove trailing / (except root)
/// On Windows, also converts backslashes to forward slashes
pub fn normalize_path(p: &str) -> String {
    // Convert backslashes to forward slashes on Windows
    #[cfg(windows)]
    let p = p.replace('\\', "/");

    #[cfg(not(windows))]
    let p = p.to_string();

    let p = if p.starts_with('/') {
        p
    } else {
        format!("/{p}")
    };
    if p.len() > 1 && p.ends_with('/') {
        p.trim_end_matches('/').to_string()
    } else {
        p
    }
}

/// Get parent path
pub fn parent_path(p: &str) -> String {
    let p = normalize_path(p);
    if p == "/" {
        return "/".to_string();
    }
    match p.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => p[..idx].to_string(),
        None => "/".to_string(),
    }
}

/// Get basename
pub fn basename(p: &str) -> String {
    let p = normalize_path(p);
    if p == "/" {
        return "/".to_string();
    }
    p.rsplit('/').next().unwrap_or("").to_string()
}
