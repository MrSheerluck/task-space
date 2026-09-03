//! Shared Task Space document schema.
//!
//! The browser currently persists this model locally. Keeping the schema in
//! this crate means the future API and sync worker can consume the exact same
//! document without copying the UI's private types.

use serde::{Deserialize, Serialize};

pub mod crdt;
pub mod sync;

pub const CURRENT_SCHEMA_VERSION: u32 = 2;
pub type EntityId = u64;

fn current_schema_version() -> u32 {
    CURRENT_SCHEMA_VERSION
}

fn empty_string() -> String {
    String::new()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
pub enum NoteStatus {
    #[default]
    Todo,
    InProgress,
    Done,
}

impl NoteStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Todo => "to do",
            Self::InProgress => "in progress",
            Self::Done => "done",
        }
    }

    pub fn mark(self) -> &'static str {
        match self {
            Self::Todo => "○",
            Self::InProgress => "◐",
            Self::Done => "✓",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::Todo => Self::InProgress,
            Self::InProgress => Self::Done,
            Self::Done => Self::Todo,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
pub enum NoteColor {
    #[default]
    Yellow,
    Pink,
    Blue,
    Green,
    Lavender,
}

impl NoteColor {
    pub fn next(self) -> Self {
        match self {
            Self::Yellow => Self::Pink,
            Self::Pink => Self::Blue,
            Self::Blue => Self::Green,
            Self::Green => Self::Lavender,
            Self::Lavender => Self::Yellow,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Note {
    pub id: EntityId,
    #[serde(default = "empty_string")]
    pub text: String,
    pub color: NoteColor,
    #[serde(default)]
    pub status: NoteStatus,
    #[serde(default)]
    pub due_date: Option<String>,
    pub x: f64,
    pub y: f64,
    pub rotation: i8,
    #[serde(default)]
    pub group_id: Option<EntityId>,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub deleted_at: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Group {
    pub id: EntityId,
    #[serde(default = "empty_string")]
    pub label: String,
    #[serde(default)]
    pub origin: Option<(f64, f64)>,
    #[serde(default)]
    pub size: Option<(f64, f64)>,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub deleted_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum TombstoneKind {
    Note,
    Group,
    Space,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Tombstone {
    pub kind: TombstoneKind,
    pub id: EntityId,
    pub deleted_at: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BoardData {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub notes: Vec<Note>,
    #[serde(default)]
    pub groups: Vec<Group>,
    #[serde(default)]
    pub tombstones: Vec<Tombstone>,
}

impl Default for BoardData {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            notes: Vec::new(),
            groups: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Space {
    pub id: EntityId,
    pub name: String,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub deleted_at: Option<u64>,
    pub board: BoardData,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkspaceData {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub device_id: String,
    #[serde(default)]
    pub tombstones: Vec<Tombstone>,
    pub spaces: Vec<Space>,
    pub active_space_id: EntityId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_board_json_gets_current_defaults() {
        let board: BoardData = serde_json::from_str(r#"{"notes":[],"groups":[]}"#)
            .expect("legacy board should deserialize");

        assert_eq!(board.schema_version, CURRENT_SCHEMA_VERSION);
        assert!(board.tombstones.is_empty());
    }

    #[test]
    fn tombstones_round_trip_with_board_data() {
        let board = BoardData {
            tombstones: vec![Tombstone {
                kind: TombstoneKind::Note,
                id: 42,
                deleted_at: 123,
            }],
            ..Default::default()
        };
        let raw = serde_json::to_string(&board).expect("board should serialize");
        let restored: BoardData = serde_json::from_str(&raw).expect("board should deserialize");

        assert_eq!(restored, board);
    }
}
