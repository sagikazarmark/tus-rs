use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::IsTerminal;
use tus_client::UploadProgress;

pub(crate) struct Progress {
    bar: ProgressBar,
    total: u64,
    interactive: bool,
}

impl Progress {
    pub(crate) fn new(total: u64) -> Self {
        let interactive = std::io::stderr().is_terminal();
        let bar = if interactive {
            let bar = ProgressBar::new(total);
            bar.set_draw_target(ProgressDrawTarget::stderr());
            bar.set_style(
                ProgressStyle::with_template("{wide_bar} {bytes}/{total_bytes} ({bytes_per_sec})")
                    .expect("progress template must be valid")
                    .progress_chars("=> "),
            );
            bar
        } else {
            ProgressBar::hidden()
        };

        Self {
            bar,
            total,
            interactive,
        }
    }

    pub(crate) fn finish(&self, uploaded: u64) {
        let uploaded = uploaded.min(self.total);
        self.bar.set_position(uploaded);
        if self.interactive {
            self.bar
                .finish_with_message(format!("uploaded {uploaded}/{} bytes", self.total));
        }
    }
}

impl UploadProgress for Progress {
    fn on_progress(&mut self, uploaded: u64, total: u64) {
        self.total = total;
        self.bar.set_length(total);
        self.bar.set_position(uploaded);
    }
}
