//! Path utilities for MDS.

use std::borrow::Cow;

/// Normalize path: ensure leading `/`, remove trailing `/` (except root).
/// On Windows, converts backslashes to forward slashes.
pub fn normalize_path(p: &str) -> String {
    let p: Cow<str> = if cfg!(windows) {
        Cow::Owned(p.replace('\\', "/"))
    } else {
        Cow::Borrowed(p)
    };
    let p = if p.starts_with('/') {
        p.into_owned()
    } else {
        format!("/{p}")
    };
    if p.len() > 1 && p.ends_with('/') {
        p.trim_end_matches('/').to_string()
    } else {
        p
    }
}

/// Parent path: `"/a/b/c"` → `"/a/b"`, `"/a"` → `"/"`.
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

/// Basename: `"/a/b/c"` → `"c"`, `"/"` → `"/"`.
pub fn basename(p: &str) -> String {
    let p = normalize_path(p);
    if p == "/" {
        return "/".to_string();
    }
    p.rsplit('/').next().unwrap_or("").to_string()
}
