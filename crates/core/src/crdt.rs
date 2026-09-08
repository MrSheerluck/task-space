//! Yrs document storage for local-first and synchronized workspaces.
//!
//! The UI continues to work with the serde models from the parent module. This
//! module is the boundary between that convenient projection and the canonical
//! CRDT representation used for sync. JSON exports intentionally remain a
//! separate, human-readable format.

use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Any, Doc, Map, MapRef, Out, ReadTxn, StateVector, Transact, Update, WriteTxn};

use crate::sync::{MAX_SYNC_SNAPSHOT_BYTES, MAX_SYNC_UPDATE_BYTES};
use crate::{BoardData, Group, Note, NoteColor, NoteStatus, TombstoneKind};

pub const CRDT_SCHEMA_VERSION: i64 = 1;
pub const ROOT_META: &str = "meta";
pub const ROOT_NOTES: &str = "notes";
pub const ROOT_GROUPS: &str = "groups";

/// A Yrs document containing one space's shared state.
#[derive(Clone)]
pub struct SpaceDoc {
    doc: Doc,
}

impl Default for SpaceDoc {
    fn default() -> Self {
        Self::new()
    }
}

impl SpaceDoc {
    pub fn new() -> Self {
        let doc = Doc::new();
        let mut txn = doc.transact_mut();
        let meta = txn.get_or_insert_map(ROOT_META);
        meta.try_update(&mut txn, "schema_version", CRDT_SCHEMA_VERSION);
        txn.get_or_insert_map(ROOT_NOTES);
        txn.get_or_insert_map(ROOT_GROUPS);
        drop(txn);
        Self { doc }
    }

    pub fn from_update(update: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if update.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err("CRDT snapshot is too large".into());
        }
        // Apply the incoming snapshot to a blank document first. Seeding the
        // current schema before applying it lets Yrs conflict resolution hide
        // a future schema marker from the remote document.
        let space = Self { doc: Doc::new() };
        {
            let mut txn = space.doc.transact_mut();
            // Yrs updates address these named roots, so create the roots but
            // do not seed the schema value until after the remote update has
            // been validated.
            txn.get_or_insert_map(ROOT_META);
            txn.get_or_insert_map(ROOT_NOTES);
            txn.get_or_insert_map(ROOT_GROUPS);
        }
        let mut txn = space.doc.transact_mut();
        txn.apply_update(Update::decode_v1(update)?)?;
        drop(txn);
        let stored_schema = {
            let txn = space.doc.transact();
            match txn.get(ROOT_META) {
                None => None,
                Some(Out::YMap(meta)) => match meta.get(&txn, "schema_version") {
                    None => None,
                    Some(Out::Any(Any::Number(version))) => Some(
                        parse_schema_number(version).ok_or("invalid CRDT schema version marker")?,
                    ),
                    Some(Out::Any(Any::BigInt(version))) => Some(version),
                    Some(_) => {
                        return Err("invalid CRDT schema version marker".into());
                    }
                },
                Some(_) => return Err("invalid CRDT metadata root".into()),
            }
        };
        if stored_schema
            .as_ref()
            .is_some_and(|version| *version != CRDT_SCHEMA_VERSION)
        {
            return Err(format!(
                "unsupported CRDT schema version {}",
                stored_schema.unwrap_or_default()
            )
            .into());
        }
        let mut txn = space.doc.transact_mut();
        let has_schema_marker = txn
            .get(ROOT_META)
            .and_then(|value| match value {
                Out::YMap(meta) => meta.get(&txn, "schema_version"),
                _ => None,
            })
            .is_some();
        if !has_schema_marker {
            let meta = txn.get_or_insert_map(ROOT_META);
            meta.try_update(&mut txn, "schema_version", CRDT_SCHEMA_VERSION);
        }
        txn.get_or_insert_map(ROOT_NOTES);
        txn.get_or_insert_map(ROOT_GROUPS);
        drop(txn);
        Ok(space)
    }

    pub fn schema_version(&self) -> i64 {
        let txn = self.doc.transact();
        txn.get(ROOT_META)
            .and_then(|value| match value {
                Out::YMap(meta) => meta.get(&txn, "schema_version"),
                _ => None,
            })
            .and_then(|value| match value {
                Out::Any(Any::Number(version)) => parse_schema_number(version),
                Out::Any(Any::BigInt(version)) => Some(version),
                _ => None,
            })
            .unwrap_or(CRDT_SCHEMA_VERSION)
    }

    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    pub fn state_vector(&self) -> Vec<u8> {
        self.doc.transact().state_vector().encode_v1()
    }

    /// Encode the empty state vector used when a replica has never uploaded
    /// any of its local document state.
    pub fn empty_state_vector() -> Vec<u8> {
        StateVector::default().encode_v1()
    }

    pub fn encode_update(
        &self,
        state_vector: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let state_vector = StateVector::decode_v1(state_vector)?;
        Ok(self.doc.transact().encode_state_as_update_v1(&state_vector))
    }

    pub fn apply_update(
        &self,
        update: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if update.len() > MAX_SYNC_UPDATE_BYTES {
            return Err("CRDT update is too large".into());
        }
        let mut txn = self.doc.transact_mut();
        txn.apply_update(Update::decode_v1(update)?)?;
        Ok(())
    }

    /// Encode the full current state. The resulting bytes are suitable as a
    /// compact IndexedDB snapshot or as an initial server snapshot.
    pub fn snapshot(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// Replace the document with the current board projection. This is used
    /// once when migrating the existing JSON/IndexedDB format.
    pub fn import_board(&self, board: &BoardData) {
        let mut txn = self.doc.transact_mut();
        let meta = txn.get_or_insert_map(ROOT_META);
        meta.try_update(&mut txn, "schema_version", CRDT_SCHEMA_VERSION);

        let notes = txn.get_or_insert_map(ROOT_NOTES);
        for note in &board.notes {
            let note_map: MapRef = notes.get_or_init(&mut txn, note.id.to_string());
            sync_note_map(&mut txn, &note_map, note);
        }

        let groups = txn.get_or_insert_map(ROOT_GROUPS);
        for group in &board.groups {
            let group_map: MapRef = groups.get_or_init(&mut txn, group.id.to_string());
            sync_group_map(&mut txn, &group_map, group);
        }

        for tombstone in &board.tombstones {
            let (collection, key) = match tombstone.kind {
                TombstoneKind::Note => (&notes, tombstone.id.to_string()),
                TombstoneKind::Group => (&groups, tombstone.id.to_string()),
                TombstoneKind::Space => continue,
            };
            if let Some(map) = collection.get(&txn, &key).and_then(out_map) {
                map.try_update(&mut txn, "deleted_at", tombstone.deleted_at as i64);
            }
        }
    }

    pub fn board(&self) -> BoardData {
        let txn = self.doc.transact();
        let notes = txn
            .get(ROOT_NOTES)
            .and_then(out_map)
            .map(|map| map_to_notes(&txn, &map))
            .unwrap_or_default();
        let groups = txn
            .get(ROOT_GROUPS)
            .and_then(out_map)
            .map(|map| map_to_groups(&txn, &map))
            .unwrap_or_default();

        BoardData {
            schema_version: crate::CURRENT_SCHEMA_VERSION,
            notes,
            groups,
            // Deletions are represented in the Yrs maps until compaction. The
            // projection intentionally excludes deleted entities.
            tombstones: Vec::new(),
        }
    }

    pub fn set_note_field(&self, note_id: u64, field: &str, value: Any) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(note) = txn
            .get(ROOT_NOTES)
            .and_then(out_map)
            .and_then(|notes| notes.get(&txn, &note_id.to_string()).and_then(out_map))
        else {
            return false;
        };

        note.insert(&mut txn, field, value);
        true
    }

    pub fn delete_note(&self, note_id: u64, deleted_at: u64) -> bool {
        self.set_note_field(note_id, "deleted_at", Any::from(deleted_at as i64))
    }

    pub fn delete_group(&self, group_id: u64, deleted_at: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(group) = txn
            .get(ROOT_GROUPS)
            .and_then(out_map)
            .and_then(|groups| groups.get(&txn, &group_id.to_string()).and_then(out_map))
        else {
            return false;
        };

        group.insert(&mut txn, "deleted_at", Any::from(deleted_at as i64));
        true
    }
}

fn parse_schema_number(version: f64) -> Option<i64> {
    (version.is_finite() && version >= 0.0 && version.fract() == 0.0 && version <= i64::MAX as f64)
        .then_some(version as i64)
}

fn sync_note_map(txn: &mut yrs::TransactionMut, map: &MapRef, note: &Note) {
    map.try_update(txn, "id", note.id as i64);
    map.try_update(txn, "text", note.text.clone());
    map.try_update(txn, "color", note_color_name(note.color));
    map.try_update(txn, "status", note_status_name(note.status));
    map.try_update(txn, "x", note.x);
    map.try_update(txn, "y", note.y);
    map.try_update(txn, "rotation", note.rotation as i64);
    map.try_update(txn, "created_at", note.created_at as i64);
    map.try_update(txn, "updated_at", note.updated_at as i64);
    map.try_update(txn, "due_date", option_string(note.due_date.as_deref()));
    map.try_update(txn, "group_id", option_number(note.group_id));
    map.try_update(txn, "deleted_at", option_number(note.deleted_at));
}

fn sync_group_map(txn: &mut yrs::TransactionMut, map: &MapRef, group: &Group) {
    map.try_update(txn, "id", group.id as i64);
    map.try_update(txn, "label", group.label.clone());
    map.try_update(txn, "created_at", group.created_at as i64);
    map.try_update(txn, "updated_at", group.updated_at as i64);
    map.try_update(txn, "deleted_at", option_number(group.deleted_at));
    let (origin_x, origin_y) = group.origin.map_or((Any::Null, Any::Null), |(x, y)| {
        (Any::from(x), Any::from(y))
    });
    map.try_update(txn, "origin_x", origin_x);
    map.try_update(txn, "origin_y", origin_y);
    let (size_width, size_height) = group
        .size
        .map_or((Any::Null, Any::Null), |(width, height)| {
            (Any::from(width), Any::from(height))
        });
    map.try_update(txn, "size_width", size_width);
    map.try_update(txn, "size_height", size_height);
}

fn map_to_notes<T: ReadTxn>(txn: &T, notes: &MapRef) -> Vec<Note> {
    let mut result: Vec<_> = notes
        .iter(txn)
        .filter_map(|(id, value)| out_map(value).and_then(|map| map_to_note(txn, id, &map)))
        .collect();
    result.sort_by_key(|note| note.id);
    result
}

fn map_to_groups<T: ReadTxn>(txn: &T, groups: &MapRef) -> Vec<Group> {
    let mut result: Vec<_> = groups
        .iter(txn)
        .filter_map(|(id, value)| out_map(value).and_then(|map| map_to_group(txn, id, &map)))
        .collect();
    result.sort_by_key(|group| group.id);
    result
}

fn out_map(value: yrs::Out) -> Option<MapRef> {
    match value {
        yrs::Out::YMap(map) => Some(map),
        _ => None,
    }
}

fn map_to_note<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Note> {
    let id = number(map.get(txn, "id")).or_else(|| key.parse::<u64>().ok())?;
    let deleted_at = number(map.get(txn, "deleted_at"));
    if deleted_at.is_some() {
        return None;
    }
    Some(Note {
        id,
        text: string(map.get(txn, "text")).unwrap_or_default(),
        color: parse_color(string(map.get(txn, "color")).as_deref()),
        status: parse_status(string(map.get(txn, "status")).as_deref()),
        due_date: string(map.get(txn, "due_date")),
        x: decimal(map.get(txn, "x")).unwrap_or_default(),
        y: decimal(map.get(txn, "y")).unwrap_or_default(),
        rotation: signed_i8(map.get(txn, "rotation")).unwrap_or_default(),
        group_id: number(map.get(txn, "group_id")),
        created_at: number(map.get(txn, "created_at")).unwrap_or_default(),
        updated_at: number(map.get(txn, "updated_at")).unwrap_or_default(),
        deleted_at,
    })
}

fn map_to_group<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Group> {
    let id = number(map.get(txn, "id")).or_else(|| key.parse::<u64>().ok())?;
    let deleted_at = number(map.get(txn, "deleted_at"));
    if deleted_at.is_some() {
        return None;
    }
    Some(Group {
        id,
        label: string(map.get(txn, "label")).unwrap_or_default(),
        origin: pair(map.get(txn, "origin_x"), map.get(txn, "origin_y")),
        size: pair(map.get(txn, "size_width"), map.get(txn, "size_height")),
        created_at: number(map.get(txn, "created_at")).unwrap_or_default(),
        updated_at: number(map.get(txn, "updated_at")).unwrap_or_default(),
        deleted_at,
    })
}

fn string(value: Option<yrs::Out>) -> Option<String> {
    value.and_then(|value| match value {
        yrs::Out::Any(Any::String(value)) => Some(value.to_string()),
        _ => None,
    })
}

fn number(value: Option<yrs::Out>) -> Option<u64> {
    value.and_then(|value| match value {
        yrs::Out::Any(Any::Number(value))
            if value.is_finite()
                && value >= 0.0
                && value.fract() == 0.0
                && value <= u64::MAX as f64 =>
        {
            Some(value as u64)
        }
        yrs::Out::Any(Any::BigInt(value)) if value >= 0 => u64::try_from(value).ok(),
        _ => None,
    })
}

fn signed_i8(value: Option<yrs::Out>) -> Option<i8> {
    value.and_then(|value| match value {
        yrs::Out::Any(Any::Number(value))
            if value.is_finite()
                && value.fract() == 0.0
                && value >= f64::from(i8::MIN)
                && value <= f64::from(i8::MAX) =>
        {
            Some(value as i8)
        }
        yrs::Out::Any(Any::BigInt(value)) => i8::try_from(value).ok(),
        _ => None,
    })
}

fn decimal(value: Option<yrs::Out>) -> Option<f64> {
    value.and_then(|value| match value {
        yrs::Out::Any(Any::Number(value)) if value.is_finite() => Some(value),
        _ => None,
    })
}

fn pair(first: Option<yrs::Out>, second: Option<yrs::Out>) -> Option<(f64, f64)> {
    Some((decimal(first)?, decimal(second)?))
}

fn option_string(value: Option<&str>) -> Any {
    value.map_or(Any::Null, |value| Any::from(value.to_string()))
}

fn option_number(value: Option<u64>) -> Any {
    value.map_or(Any::Null, |value| Any::from(value as i64))
}

fn note_color_name(color: NoteColor) -> &'static str {
    match color {
        NoteColor::Yellow => "yellow",
        NoteColor::Pink => "pink",
        NoteColor::Blue => "blue",
        NoteColor::Green => "green",
        NoteColor::Lavender => "lavender",
    }
}

fn parse_color(value: Option<&str>) -> NoteColor {
    match value {
        Some("pink") => NoteColor::Pink,
        Some("blue") => NoteColor::Blue,
        Some("green") => NoteColor::Green,
        Some("lavender") => NoteColor::Lavender,
        _ => NoteColor::Yellow,
    }
}

fn note_status_name(status: NoteStatus) -> &'static str {
    match status {
        NoteStatus::Todo => "todo",
        NoteStatus::InProgress => "in_progress",
        NoteStatus::Done => "done",
    }
}

fn parse_status(value: Option<&str>) -> NoteStatus {
    match value {
        Some("in_progress") => NoteStatus::InProgress,
        Some("done") => NoteStatus::Done,
        _ => NoteStatus::Todo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BoardData, Note};

    fn note(id: u64, text: &str) -> Note {
        Note {
            id,
            text: text.into(),
            x: 10.0,
            y: 20.0,
            ..Default::default()
        }
    }

    #[test]
    fn board_round_trips_through_yrs_snapshot() {
        let board = BoardData {
            notes: vec![note(7, "shared note")],
            ..Default::default()
        };
        let local = SpaceDoc::new();
        local.import_board(&board);

        let restored = SpaceDoc::from_update(&local.snapshot()).expect("snapshot should apply");
        assert_eq!(restored.board().notes, board.notes);
    }

    #[test]
    fn signed_rotation_and_group_reference_round_trip() {
        let board = BoardData {
            notes: vec![Note {
                id: 7,
                rotation: -3,
                group_id: Some(9),
                ..note(7, "grouped")
            }],
            groups: vec![crate::Group {
                id: 9,
                label: "group".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let local = SpaceDoc::new();
        local.import_board(&board);
        let restored = SpaceDoc::from_update(&local.snapshot()).expect("snapshot should apply");
        let restored_note = &restored.board().notes[0];
        assert_eq!(restored_note.rotation, -3);
        assert_eq!(restored_note.group_id, Some(9));
    }

    #[test]
    fn new_documents_are_tagged_with_the_supported_schema() {
        assert_eq!(SpaceDoc::new().schema_version(), CRDT_SCHEMA_VERSION);
    }

    #[test]
    fn documents_with_an_unknown_schema_are_rejected() {
        let document = SpaceDoc::new();
        document.import_board(&BoardData {
            notes: vec![note(99, "schema marker")],
            ..Default::default()
        });
        {
            let mut transaction = document.doc().transact_mut();
            let meta = transaction.get_or_insert_map(ROOT_META);
            meta.try_update(&mut transaction, "schema_version", CRDT_SCHEMA_VERSION + 1);
        }
        assert_eq!(document.schema_version(), CRDT_SCHEMA_VERSION + 1);

        let raw = document.snapshot();
        let result = SpaceDoc::from_update(&raw);
        assert!(
            result.is_err(),
            "future schema should require an explicit migration"
        );
        assert!(result.err().is_some_and(|error| {
            error
                .to_string()
                .contains("unsupported CRDT schema version")
        }));
    }

    #[test]
    fn documents_with_a_fractional_schema_marker_are_rejected() {
        let document = SpaceDoc::new();
        {
            let mut transaction = document.doc().transact_mut();
            let meta = transaction.get_or_insert_map(ROOT_META);
            meta.try_update(&mut transaction, "schema_version", 1.5_f64);
        }
        let result = SpaceDoc::from_update(&document.snapshot());
        assert!(result.is_err());
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("invalid CRDT schema"))
        );
    }

    #[test]
    fn replicas_converge_after_exchange() {
        let first = SpaceDoc::new();
        first.import_board(&BoardData {
            notes: vec![note(1, "original")],
            ..Default::default()
        });
        let second = SpaceDoc::from_update(&first.snapshot()).expect("initial state should apply");

        first.set_note_field(1, "text", Any::from("first edit"));
        second.set_note_field(1, "due_date", Any::from("2026-09-10"));

        let first_update = first
            .encode_update(&second.state_vector())
            .expect("first diff should encode");
        let second_update = second
            .encode_update(&first.state_vector())
            .expect("second diff should encode");
        first
            .apply_update(&second_update)
            .expect("second diff should apply");
        second
            .apply_update(&first_update)
            .expect("first diff should apply");

        assert_eq!(first.board(), second.board());
        let merged = &first.board().notes[0];
        assert_eq!(merged.text, "first edit");
        assert_eq!(merged.due_date.as_deref(), Some("2026-09-10"));
    }

    #[test]
    fn deleted_entities_are_hidden_but_remain_in_crdt_history() {
        let doc = SpaceDoc::new();
        doc.import_board(&BoardData {
            notes: vec![note(2, "remove me")],
            ..Default::default()
        });
        assert!(doc.delete_note(2, 99));
        assert!(doc.board().notes.is_empty());
        assert!(!doc.snapshot().is_empty());
    }

    #[test]
    fn imported_tombstones_hide_deleted_entities_and_restore_can_clear_them() {
        let doc = SpaceDoc::new();
        doc.import_board(&BoardData {
            notes: vec![note(3, "recoverable")],
            tombstones: vec![crate::Tombstone {
                kind: crate::TombstoneKind::Note,
                id: 3,
                deleted_at: 100,
            }],
            ..Default::default()
        });
        assert!(doc.board().notes.is_empty());

        doc.import_board(&BoardData {
            notes: vec![note(3, "recoverable")],
            ..Default::default()
        });
        assert_eq!(doc.board().notes[0].text, "recoverable");
    }
}
