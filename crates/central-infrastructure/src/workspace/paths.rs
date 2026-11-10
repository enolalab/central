//! Path containment for Git workspaces (P3 Task 5).
//!
//! Every repo-relative path Central accepts is validated here before any
//! filesystem or git access: canonical comparison when the path exists (so
//! a junction or symlink inside the root cannot smuggle it out), lexical
//! `..`/prefix checks otherwise. Windows device prefixes (`\\?\`, `\\.\`),
//! drive-absolute paths, and NTFS alternate streams (`:`) fail closed.
//! Callers pass argv only — never shell strings — so contained paths cannot
//! be re-parsed on the way to git.

use std::path::{Path, PathBuf};

use central_application::contracts::AppError;

/// Canonicalize `raw` as a repository location: it must exist and denote a
/// directory. Returned roots are canonical so later comparisons are stable.
pub fn canonical_root(raw: &Path) -> Result<PathBuf, AppError> {
    let canonical = raw.canonicalize().map_err(|error| {
        AppError::Validation(format!(
            "workspace root is unusable: {}: {error}",
            raw.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(AppError::Validation(format!(
            "workspace root is not a directory: {}",
            raw.display()
        )));
    }
    Ok(canonical)
}

/// Resolve `raw` under `root` and refuse anything that escapes. `root` must
/// already be canonical (see [`canonical_root`]).
///
/// When the path (or its longest existing ancestor) resolves, the canonical
/// form must stay inside the canonical root — this is what stops a junction
/// inside the root from pointing the worktree outside it. Otherwise the
/// lexical form must stay inside: `..` climbing past the root, absolute
/// paths, drive prefixes, device namespaces and stream syntax all fail.
pub fn contained_path(root: &Path, raw: &str) -> Result<PathBuf, AppError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "workspace path must not be empty".to_string(),
        ));
    }
    reject_syntax(trimmed)?;
    let unified = trimmed.replace('\\', "/");
    let absolute = unified.starts_with('/')
        || unified
            .split('/')
            .next()
            .is_some_and(|first| first.ends_with(':'));
    if absolute {
        // Absolute inputs survive only when canonical resolution proves
        // them inside the root; missing paths fail closed.
        return resolve_absolute(root, trimmed);
    }
    let Some(lexical) = normalize_segments(Some(root), &unified) else {
        return Err(AppError::Forbidden(format!(
            "workspace path escapes the root: {trimmed}"
        )));
    };
    if lexical != root && !lexical.starts_with(root) {
        return Err(AppError::Forbidden(format!(
            "workspace path escapes the root: {trimmed}"
        )));
    }
    // Prefer the canonical comparison: walk up to the longest existing
    // ancestor so a junction (or symlink) at any level resolves before the
    // containment check.
    let mut existing = lexical.as_path();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if existing.exists() {
            let resolved = existing.canonicalize().map_err(|error| {
                AppError::Storage(format!(
                    "cannot resolve workspace path {}: {error}",
                    existing.display()
                ))
            })?;
            let mut full = resolved;
            for part in tail.iter().rev() {
                full.push(part);
            }
            let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
            if full == canonical_root || full.starts_with(&canonical_root) {
                return Ok(lexical);
            }
            return Err(AppError::Forbidden(format!(
                "workspace path escapes the root: {trimmed}"
            )));
        }
        match existing.parent() {
            Some(parent) if parent != existing => {
                if let Some(name) = existing.file_name() {
                    tail.push(name.to_os_string());
                }
                existing = parent;
            }
            _ => {
                // Nothing exists up to the filesystem root: the lexical
                // form already proved containment above.
                return Ok(lexical);
            }
        }
    }
}

/// Fail Windows-unsafe syntax before any resolution: device namespaces,
/// drive prefixes and NTFS alternate data streams.
fn reject_syntax(trimmed: &str) -> Result<(), AppError> {
    if trimmed.contains('\0') {
        return Err(AppError::Validation(
            "workspace path contains NUL".to_string(),
        ));
    }
    let unified = trimmed.replace('\\', "/");
    if unified.starts_with("//?/") || unified.starts_with("//./") {
        return Err(AppError::Forbidden(format!(
            "workspace path uses a device namespace: {trimmed}"
        )));
    }
    if unified.contains(':') {
        return Err(AppError::Forbidden(format!(
            "workspace path escapes the root: {trimmed}"
        )));
    }
    Ok(())
}

/// Resolve an absolute `raw` against `root`: the longest existing ancestor
/// canonicalizes and must land inside the canonical root. Anything missing
/// fails closed — absolute paths are never trusted lexically.
fn resolve_absolute(root: &Path, trimmed: &str) -> Result<PathBuf, AppError> {
    let unified = trimmed.replace('\\', "/");
    let Some(candidate) = normalize_segments(None, &unified) else {
        return Err(AppError::Forbidden(format!(
            "workspace path escapes the root: {trimmed}"
        )));
    };
    let mut existing = candidate.as_path();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if existing.exists() {
            let resolved = existing.canonicalize().map_err(|error| {
                AppError::Storage(format!(
                    "cannot resolve workspace path {}: {error}",
                    existing.display()
                ))
            })?;
            let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
            if resolved == canonical_root || resolved.starts_with(&canonical_root) {
                let mut full = resolved;
                for part in tail.iter().rev() {
                    full.push(part);
                }
                return Ok(full);
            }
            return Err(AppError::Forbidden(format!(
                "workspace path escapes the root: {trimmed}"
            )));
        }
        match existing.parent() {
            Some(parent) if parent != existing => {
                if let Some(name) = existing.file_name() {
                    tail.push(name.to_os_string());
                }
                existing = parent;
            }
            _ => {
                return Err(AppError::Forbidden(format!(
                    "workspace path escapes the root: {trimmed}"
                )));
            }
        }
    }
}

/// Lexically join `raw` under `base`, resolving `.`/`..` without touching
/// the filesystem. A `..` climbing past the base returns `None`.
fn normalize_segments(base: Option<&Path>, raw: &str) -> Option<PathBuf> {
    let mut out = base.map_or_else(PathBuf::new, Path::to_path_buf);
    for part in raw.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if !out.pop() {
                    return None;
                }
            }
            _ => out.push(part),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_escapes_fail_without_touching_fs() {
        let root = Path::new("C:/repo");
        assert!(contained_path(root, "../outside").is_err());
        assert!(contained_path(root, "a/../../outside").is_err());
        assert!(contained_path(root, "").is_err());
        assert!(contained_path(root, "C:/other/repo").is_err());
        assert!(contained_path(root, "\\\\?\\C:\\repo").is_err());
        assert!(contained_path(root, "file:stream").is_err());
    }
}
