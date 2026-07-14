//! Presentation helpers specific to upload examples.

/// Pretty-prints a bytes-per-second value as `1.2 MB/s`, `350 KB/s`, etc.
pub fn format_bytes_per_sec(bps: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    if bps >= MB {
        format!("{:.1} MB/s", bps / MB)
    } else if bps >= KB {
        format!("{:.0} KB/s", bps / KB)
    } else {
        format!("{bps:.0} B/s")
    }
}

/// Pretty-prints a byte count as `4.0 MB`, `512 KB`, etc.
pub fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let bytes_float = bytes as f64;
    if bytes_float >= MB {
        format!("{:.1} MB", bytes_float / MB)
    } else if bytes_float >= KB {
        format!("{:.0} KB", bytes_float / KB)
    } else {
        format!("{bytes} B")
    }
}

/// Pretty-prints an ETA in seconds as `45s left`, `2m left`, etc.
pub fn format_eta(seconds: Option<f64>) -> String {
    match seconds {
        Some(seconds) if seconds.is_finite() && seconds > 0.0 => {
            if seconds < 60.0 {
                format!("{}s left", seconds as u64)
            } else if seconds < 3600.0 {
                format!("{}m left", (seconds / 60.0) as u64)
            } else {
                format!("{:.1}h left", seconds / 3600.0)
            }
        }
        _ => "n/a".to_string(),
    }
}
