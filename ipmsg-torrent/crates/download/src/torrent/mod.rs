//! BitTorrent protocol implementation

pub mod bencode;
mod engine;
pub mod file_selection;
pub mod meta;
pub mod metadata;
mod peer;
mod tracker;

pub use engine::{DownloadError, TorrentEngine};
pub use file_selection::{FileEntry, FileSelection, FileSelectionError, SelectionMode};
pub use meta::{TorrentError, TorrentMeta};
pub use metadata::{MetadataError, MetadataFetcher};
pub use tracker::{AnnounceEvent, HttpTracker, TrackerPeer};
