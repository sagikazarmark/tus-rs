use chrono::{DateTime, Utc};

use crate::state::UploadState;

pub(crate) struct ProtocolExpiration {
    expires_at: Option<DateTime<Utc>>,
    is_complete: bool,
    is_partial: bool,
}

impl ProtocolExpiration {
    pub(crate) fn for_upload(state: &UploadState) -> Self {
        Self::from_parts(
            state.expires_at().cloned(),
            state.is_complete(),
            state.is_partial(),
        )
    }

    pub(crate) fn from_parts(
        expires_at: Option<DateTime<Utc>>,
        is_complete: bool,
        is_partial: bool,
    ) -> Self {
        Self {
            expires_at,
            is_complete,
            is_partial,
        }
    }

    pub(crate) fn expires_before(&self, before: DateTime<Utc>) -> bool {
        self.is_eligible() && self.expires_at.is_some_and(|expires| expires < before)
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.expires_before(Utc::now())
    }

    pub(crate) fn header(&self) -> Option<String> {
        if !self.is_eligible() {
            return None;
        }

        self.expires_at
            .map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
    }

    fn is_eligible(&self) -> bool {
        !self.is_complete || self.is_partial
    }
}
