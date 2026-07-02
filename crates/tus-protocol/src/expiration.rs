use chrono::{DateTime, Utc};

pub(crate) fn expires_before(
    expires_at: Option<DateTime<Utc>>,
    is_complete: bool,
    is_partial: bool,
    before: DateTime<Utc>,
) -> bool {
    expiration_is_eligible(is_complete, is_partial)
        && expires_at.is_some_and(|expires| expires < before)
}

pub(crate) fn is_expired(
    expires_at: Option<DateTime<Utc>>,
    is_complete: bool,
    is_partial: bool,
) -> bool {
    expires_before(expires_at, is_complete, is_partial, Utc::now())
}

pub(crate) fn expires_header(
    expires_at: Option<DateTime<Utc>>,
    is_complete: bool,
    is_partial: bool,
) -> Option<String> {
    if !expiration_is_eligible(is_complete, is_partial) {
        return None;
    }

    expires_at.map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
}

fn expiration_is_eligible(is_complete: bool, is_partial: bool) -> bool {
    !is_complete || is_partial
}
