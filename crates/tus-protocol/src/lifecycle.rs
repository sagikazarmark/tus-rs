//! Upload lifecycle transitions and policy.
//!
//! Low-level parsing helpers are not public API:
//!
//! ```compile_fail
//! let _ = tus_protocol::lifecycle::extract_partial_id("/files/part", "/files");
//! ```

mod creation;
mod final_upload;
mod finish;
mod receive;
mod recovery;

pub use creation::{CreationRequest, CreationTransition, prepare_creation};
pub use final_upload::{
    FinalUploadPlan, FinalUploadStatus, load_final_upload_plan, load_final_upload_status,
    summarize_final_parts,
};
pub(crate) use final_upload::{
    create_final_upload, final_upload_response_facts, repair_final_upload,
};
pub use finish::run_pre_finish;
pub use receive::{
    ReceiveProjection, ReceiveRequest, apply_receive_commit, prepare_receive,
    receive_body_size_limit, validate_receive_body,
};
pub(crate) use recovery::reconcile_state_offset;

use crate::error::{Error, Result};
use crate::state::UploadState;

/// Rejects expired uploads before protocol handlers expose or mutate state.
pub fn ensure_active(state: &UploadState) -> Result<()> {
    if state.is_expired() {
        return Err(Error::Expired(state.id().to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    #[test]
    fn ensure_active_accepts_upload_without_expiration() {
        let state = UploadState::new("upload-1");

        ensure_active(&state).unwrap();
    }

    #[test]
    fn ensure_active_rejects_expired_upload() {
        let state = UploadState::new("upload-1").with_expiration(Utc::now() - Duration::seconds(1));

        let err = ensure_active(&state).unwrap_err();

        assert!(matches!(err, Error::Expired(id) if id == "upload-1"));
    }
}
