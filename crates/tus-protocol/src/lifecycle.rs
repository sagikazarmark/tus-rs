//! Upload lifecycle transitions and policy.
//!
//! Low-level parsing helpers are not public API:
//!
//! ```compile_fail
//! let _ = tus_protocol::lifecycle::extract_partial_id("/files/part", "/files");
//! ```

mod access;
mod creation;
mod final_upload;
mod finish;
mod receive;
mod reclamation;
mod recovery;

pub(crate) use access::{prepare_upload_access, prepare_upload_mutation_access};
pub use creation::{CreationRequest, CreationTransition, prepare_creation};
pub(crate) use final_upload::FinalUploadMaterializer;
pub use finish::run_pre_finish;
pub(crate) use receive::{ReceiveBodyKind, commit_receive_body, prepare_receive_body};
pub use receive::{ReceiveRequest, prepare_receive};
pub use reclamation::{
    ExpiredUploadReclamationOutcome, ExpiredUploadReclamationReport, reclaim_expired_uploads,
};
pub(crate) use recovery::{reconcile_state_offset, reconcile_stored_completion};

use crate::error::{Error, Result};
use crate::state::UploadState;

/// Rejects protocol-expired uploads before protocol handlers expose or mutate state.
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
