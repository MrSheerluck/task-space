//! Yrs document storage for local-first and synchronized workspaces.
//!
//! The UI continues to work with the serde models from the parent module. This
//! module is the boundary between that convenient projection and the canonical
//! CRDT representation used for sync. JSON exports intentionally remain a
//! separate, human-readable format.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{
    Any, ClientID, Doc, GetString, Map, MapRef, Options, Out, ReadTxn, StateVector, Text,
    TextPrelim, Transact, Update, WriteTxn,
};

use crate::sync::{MAX_SYNC_SNAPSHOT_BYTES, MAX_SYNC_UPDATE_BYTES};
use crate::{
    BoardData, Group, Note, NoteColor, NoteStatus, TombstoneKind, is_valid_stable_id,
    legacy_entity_stable_id,
};

pub const CRDT_SCHEMA_VERSION: i64 = 4;
const LEGACY_CRDT_SCHEMA_VERSION: i64 = 1;
const LEGACY_ENTITY_SCHEMA_VERSION: i64 = 2;
pub const ROOT_META: &str = "meta";
pub const ROOT_NOTES: &str = "notes";
pub const ROOT_GROUPS: &str = "groups";
const ENTITY_LIFECYCLE: &str = "__lifecycle";
const DELETE_OPERATION_PREFIX: &str = "delete:";
const RESTORE_OPERATION_PREFIX: &str = "restore:";

fn new_crdt_doc() -> Doc {
    // Keep deleted Yrs blocks until the server has explicitly passed the
    // supported offline/tombstone retention window. Default Yrs GC is local
    // and eager; allowing it here would make a delayed replica harder to
    // reconcile safely after a long offline period.
    Doc::with_options(Options {
        skip_gc: true,
        ..Default::default()
    })
}

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
        let doc = new_crdt_doc();
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
        let space = Self {
            doc: new_crdt_doc(),
        };
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
        if stored_schema.as_ref().is_some_and(|version| {
            *version < LEGACY_CRDT_SCHEMA_VERSION || *version > CRDT_SCHEMA_VERSION
        }) {
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
        let needs_text_migration =
            stored_schema.is_none() || stored_schema == Some(LEGACY_CRDT_SCHEMA_VERSION);
        let needs_entity_migration = stored_schema.is_none()
            || stored_schema.is_some_and(|version| version <= LEGACY_ENTITY_SCHEMA_VERSION);
        let needs_lifecycle_migration = stored_schema.is_none()
            || stored_schema.is_some_and(|version| version < CRDT_SCHEMA_VERSION);
        if needs_text_migration {
            migrate_legacy_text_fields(&mut txn);
        }
        if needs_entity_migration {
            migrate_legacy_entity_ids(&mut txn);
        }
        if needs_lifecycle_migration {
            migrate_legacy_lifecycle_markers(&mut txn, space.doc.client_id());
        }
        if !has_schema_marker
            || needs_text_migration
            || needs_entity_migration
            || needs_lifecycle_migration
        {
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

    /// Apply a persisted full snapshot while retaining the same document
    /// identity. Snapshots have a larger bound than individual network
    /// updates because they are the local crash-recovery representation.
    pub fn apply_snapshot(
        &self,
        snapshot: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err("CRDT snapshot is too large".into());
        }
        let mut txn = self.doc.transact_mut();
        txn.apply_update(Update::decode_v1(snapshot)?)?;
        Ok(())
    }

    /// Encode the full current state. The resulting bytes are suitable as a
    /// compact IndexedDB snapshot or as an initial server snapshot.
    pub fn snapshot(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    /// Remove application-level tombstone records that are older than a
    /// caller-supplied retention cutoff, then force Yrs garbage collection.
    ///
    /// This is intentionally explicit instead of happening during ordinary
    /// edits or reconciliation. The server must first decide that the cutoff
    /// exceeds the supported offline and recovery windows, write the compact
    /// replacement snapshot durably, and only then prune replay history.
    /// Returns whether any application-level tombstone was removed.
    pub fn compact_tombstones(&self, deleted_before: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let mut changed = false;

        for root in [ROOT_NOTES, ROOT_GROUPS] {
            let Some(collection) = txn.get(root).and_then(out_map) else {
                continue;
            };
            let keys: Vec<String> = collection
                .iter(&txn)
                .filter_map(|(key, value)| {
                    let map = out_map(value)?;
                    if !entity_is_deleted(&txn, &map, number(map.get(&txn, "deleted_at"))) {
                        return None;
                    }
                    let deleted_at = entity_tombstone_at(&txn, &map)?;
                    (deleted_at <= deleted_before).then(|| key.to_owned())
                })
                .collect();
            for key in keys {
                if collection.remove(&mut txn, &key).is_some() {
                    changed = true;
                }
            }
        }

        // GC also squashes Yrs delete ranges created by removing the expired
        // map entries. The delete set remains part of the snapshot, so a
        // stale replica cannot resurrect a compacted key by replaying its old
        // insertion.
        txn.gc(None);
        changed
    }

    /// Replace the document with the current board projection. This is used
    /// once when migrating the existing JSON/IndexedDB format.
    pub fn import_board(&self, board: &BoardData) {
        let mut txn = self.doc.transact_mut();
        let meta = txn.get_or_insert_map(ROOT_META);
        meta.try_update(&mut txn, "schema_version", CRDT_SCHEMA_VERSION);

        let notes = txn.get_or_insert_map(ROOT_NOTES);
        for note in &board.notes {
            let note_map: MapRef = notes.get_or_init(&mut txn, note_stable_id(note));
            sync_note_map(&mut txn, &note_map, note);
        }

        let groups = txn.get_or_insert_map(ROOT_GROUPS);
        for group in &board.groups {
            let group_map: MapRef = groups.get_or_init(&mut txn, group_stable_id(group));
            sync_group_map(&mut txn, &group_map, group);
        }

        for tombstone in &board.tombstones {
            let (collection, kind) = match tombstone.kind {
                TombstoneKind::Note => (&notes, "note"),
                TombstoneKind::Group => (&groups, "group"),
                TombstoneKind::Space => continue,
            };
            let key = if is_valid_stable_id(&tombstone.stable_id) {
                tombstone.stable_id.clone()
            } else {
                legacy_entity_stable_id(kind, tombstone.id)
            };
            let map: MapRef = collection.get_or_init(&mut txn, key.clone());
            map.try_update(&mut txn, "id", tombstone.id as i64);
            map.try_update(&mut txn, "stable_id", key);
            map.try_update(&mut txn, "deleted_at", tombstone.deleted_at as i64);
        }
        migrate_legacy_lifecycle_markers(&mut txn, self.doc.client_id());
    }

    /// Apply only the changes between two UI projections to the canonical
    /// document. This is the bridge used while the UI is being migrated to
    /// mutate the CRDT directly: a stale projection cannot rewrite unrelated
    /// fields that were concurrently changed by another replica.
    pub fn apply_board_diff(&self, before: &BoardData, after: &BoardData, deleted_at: u64) {
        let mut txn = self.doc.transact_mut();
        let notes = txn.get_or_insert_map(ROOT_NOTES);
        for note in &after.notes {
            let key = note_stable_id(note);
            let map: MapRef = notes.get_or_init(&mut txn, key);
            if let Some(previous) = before.notes.iter().find(|item| item.id == note.id) {
                sync_changed_note_map(&mut txn, &map, previous, note);
            } else {
                let was_deleted =
                    entity_is_deleted(&txn, &map, number(map.get(&txn, "deleted_at")));
                sync_note_map(&mut txn, &map, note);
                if was_deleted {
                    // A previously hidden item reappearing in the local
                    // projection is an explicit local restore (for example
                    // undo). Ordinary stale imports use import_board and do
                    // not reach this branch.
                    record_restore_lifecycle(&mut txn, &map, self.doc.client_id());
                    map.insert(&mut txn, "deleted_at", Any::Null);
                }
            }
        }
        for previous in &before.notes {
            if after.notes.iter().all(|item| item.id != previous.id)
                && let Some(map) = notes.get(&txn, &note_stable_id(previous)).and_then(out_map)
            {
                record_delete_lifecycle(&mut txn, &map, self.doc.client_id(), deleted_at);
                map.insert(&mut txn, "deleted_at", deleted_at as i64);
            }
        }

        let groups = txn.get_or_insert_map(ROOT_GROUPS);
        for group in &after.groups {
            let key = group_stable_id(group);
            let map: MapRef = groups.get_or_init(&mut txn, key);
            if let Some(previous) = before.groups.iter().find(|item| item.id == group.id) {
                sync_changed_group_map(&mut txn, &map, previous, group);
            } else {
                let was_deleted =
                    entity_is_deleted(&txn, &map, number(map.get(&txn, "deleted_at")));
                sync_group_map(&mut txn, &map, group);
                if was_deleted {
                    record_restore_lifecycle(&mut txn, &map, self.doc.client_id());
                    map.insert(&mut txn, "deleted_at", Any::Null);
                }
            }
        }
        for previous in &before.groups {
            if after.groups.iter().all(|item| item.id != previous.id)
                && let Some(map) = groups
                    .get(&txn, &group_stable_id(previous))
                    .and_then(out_map)
            {
                record_delete_lifecycle(&mut txn, &map, self.doc.client_id(), deleted_at);
                map.insert(&mut txn, "deleted_at", deleted_at as i64);
            }
        }
        migrate_legacy_lifecycle_markers(&mut txn, self.doc.client_id());
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
        if field == "deleted_at" {
            return match value {
                Any::Null => self.restore_note(note_id),
                Any::Number(value) if value.is_finite() && value >= 0.0 && value.fract() == 0.0 => {
                    self.delete_note(note_id, value as u64)
                }
                Any::BigInt(value) if value >= 0 => {
                    u64::try_from(value).is_ok_and(|value| self.delete_note(note_id, value))
                }
                _ => false,
            };
        }
        let mut txn = self.doc.transact_mut();
        let Some(note) = txn
            .get(ROOT_NOTES)
            .and_then(out_map)
            .and_then(|notes| find_entity_map(&txn, &notes, note_id))
        else {
            return false;
        };

        if field == "text" {
            if let Any::String(value) = value {
                sync_text_field(&mut txn, &note, field, value.as_ref());
            } else {
                note.insert(&mut txn, field, value);
            }
        } else {
            note.insert(&mut txn, field, value);
        }
        true
    }

    pub fn delete_note(&self, note_id: u64, deleted_at: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(note) = txn
            .get(ROOT_NOTES)
            .and_then(out_map)
            .and_then(|notes| find_entity_map(&txn, &notes, note_id))
        else {
            return false;
        };
        record_delete_lifecycle(&mut txn, &note, self.doc.client_id(), deleted_at);
        note.insert(&mut txn, "deleted_at", Any::from(deleted_at as i64));
        true
    }

    pub fn delete_group(&self, group_id: u64, deleted_at: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(group) = txn
            .get(ROOT_GROUPS)
            .and_then(out_map)
            .and_then(|groups| find_entity_map(&txn, &groups, group_id))
        else {
            return false;
        };

        record_delete_lifecycle(&mut txn, &group, self.doc.client_id(), deleted_at);
        group.insert(&mut txn, "deleted_at", Any::from(deleted_at as i64));
        true
    }

    /// Restoration is intentionally explicit. Ordinary projection imports
    /// never clear a deletion tombstone, so a stale tab cannot resurrect an
    /// item merely because it still has the item in an older JSON projection.
    pub fn restore_note(&self, note_id: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(note) = txn
            .get(ROOT_NOTES)
            .and_then(out_map)
            .and_then(|notes| find_entity_map(&txn, &notes, note_id))
        else {
            return false;
        };
        record_restore_lifecycle(&mut txn, &note, self.doc.client_id());
        note.insert(&mut txn, "deleted_at", Any::Null);
        true
    }

    pub fn restore_group(&self, group_id: u64) -> bool {
        let mut txn = self.doc.transact_mut();
        let Some(group) = txn
            .get(ROOT_GROUPS)
            .and_then(out_map)
            .and_then(|groups| find_entity_map(&txn, &groups, group_id))
        else {
            return false;
        };
        record_restore_lifecycle(&mut txn, &group, self.doc.client_id());
        group.insert(&mut txn, "deleted_at", Any::Null);
        true
    }
}

fn parse_schema_number(version: f64) -> Option<i64> {
    (version.is_finite() && version >= 0.0 && version.fract() == 0.0 && version <= i64::MAX as f64)
        .then_some(version as i64)
}

fn sync_note_map(txn: &mut yrs::TransactionMut, map: &MapRef, note: &Note) {
    map.try_update(txn, "id", note.id as i64);
    map.try_update(txn, "stable_id", note_stable_id(note));
    sync_text_field(txn, map, "text", &note.text);
    map.try_update(txn, "color", note_color_name(note.color));
    map.try_update(txn, "status", note_status_name(note.status));
    map.try_update(txn, "x", note.x);
    map.try_update(txn, "y", note.y);
    map.try_update(txn, "rotation", note.rotation as i64);
    map.try_update(txn, "created_at", note.created_at as i64);
    map.try_update(txn, "updated_at", note.updated_at as i64);
    map.try_update(txn, "due_date", option_string(note.due_date.as_deref()));
    map.try_update(txn, "group_id", option_number(note.group_id));
    map.try_update(
        txn,
        "group_stable_id",
        option_string(note_group_stable_id(note).as_deref()),
    );
    if let Some(deleted_at) = note.deleted_at {
        map.try_update(txn, "deleted_at", deleted_at as i64);
    }
}

fn sync_group_map(txn: &mut yrs::TransactionMut, map: &MapRef, group: &Group) {
    map.try_update(txn, "id", group.id as i64);
    map.try_update(txn, "stable_id", group_stable_id(group));
    sync_text_field(txn, map, "label", &group.label);
    map.try_update(txn, "created_at", group.created_at as i64);
    map.try_update(txn, "updated_at", group.updated_at as i64);
    if let Some(deleted_at) = group.deleted_at {
        map.try_update(txn, "deleted_at", deleted_at as i64);
    }
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

fn sync_changed_note_map(
    txn: &mut yrs::TransactionMut,
    map: &MapRef,
    previous: &Note,
    note: &Note,
) {
    if previous.text != note.text {
        sync_text_field_delta(txn, map, "text", &previous.text, &note.text);
    }
    if previous.color != note.color {
        map.try_update(txn, "color", note_color_name(note.color));
    }
    if previous.status != note.status {
        map.try_update(txn, "status", note_status_name(note.status));
    }
    if previous.x != note.x {
        map.try_update(txn, "x", note.x);
    }
    if previous.y != note.y {
        map.try_update(txn, "y", note.y);
    }
    if previous.rotation != note.rotation {
        map.try_update(txn, "rotation", note.rotation as i64);
    }
    if previous.created_at != note.created_at {
        map.try_update(txn, "created_at", note.created_at as i64);
    }
    if previous.updated_at != note.updated_at {
        map.try_update(txn, "updated_at", note.updated_at as i64);
    }
    if previous.due_date != note.due_date {
        map.try_update(txn, "due_date", option_string(note.due_date.as_deref()));
    }
    if previous.group_id != note.group_id {
        map.try_update(txn, "group_id", option_number(note.group_id));
    }
    if previous.group_stable_id != note.group_stable_id {
        map.try_update(
            txn,
            "group_stable_id",
            option_string(note_group_stable_id(note).as_deref()),
        );
    }
    if let Some(deleted_at) = note.deleted_at
        && previous.deleted_at != note.deleted_at
    {
        map.try_update(txn, "deleted_at", deleted_at as i64);
    }
}

fn sync_changed_group_map(
    txn: &mut yrs::TransactionMut,
    map: &MapRef,
    previous: &Group,
    group: &Group,
) {
    if previous.label != group.label {
        sync_text_field_delta(txn, map, "label", &previous.label, &group.label);
    }
    if previous.origin != group.origin {
        let (x, y) = group.origin.map_or((Any::Null, Any::Null), |(x, y)| {
            (Any::from(x), Any::from(y))
        });
        map.try_update(txn, "origin_x", x);
        map.try_update(txn, "origin_y", y);
    }
    if previous.size != group.size {
        let (width, height) = group
            .size
            .map_or((Any::Null, Any::Null), |(width, height)| {
                (Any::from(width), Any::from(height))
            });
        map.try_update(txn, "size_width", width);
        map.try_update(txn, "size_height", height);
    }
    if previous.created_at != group.created_at {
        map.try_update(txn, "created_at", group.created_at as i64);
    }
    if previous.updated_at != group.updated_at {
        map.try_update(txn, "updated_at", group.updated_at as i64);
    }
    if let Some(deleted_at) = group.deleted_at
        && previous.deleted_at != group.deleted_at
    {
        map.try_update(txn, "deleted_at", deleted_at as i64);
    }
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

fn find_entity_map<T: ReadTxn>(txn: &T, collection: &MapRef, id: u64) -> Option<MapRef> {
    collection.iter(txn).find_map(|(_, value)| {
        let map = out_map(value)?;
        (number(map.get(txn, "id")) == Some(id)).then_some(map)
    })
}

fn note_stable_id(note: &Note) -> String {
    if !is_valid_stable_id(&note.stable_id) {
        legacy_entity_stable_id("note", note.id)
    } else {
        note.stable_id.clone()
    }
}

fn group_stable_id(group: &Group) -> String {
    if !is_valid_stable_id(&group.stable_id) {
        legacy_entity_stable_id("group", group.id)
    } else {
        group.stable_id.clone()
    }
}

fn note_group_stable_id(note: &Note) -> Option<String> {
    note.group_stable_id
        .as_deref()
        .filter(|value| is_valid_stable_id(value))
        .map(ToOwned::to_owned)
        .or_else(|| note.group_id.map(|id| legacy_entity_stable_id("group", id)))
}

fn map_to_note<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Note> {
    let note = read_note(txn, key, map)?;
    (!entity_is_deleted(txn, map, note.deleted_at)).then_some(note)
}

fn read_note<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Note> {
    let id = number(map.get(txn, "id")).or_else(|| key.parse::<u64>().ok())?;
    let stable_id = string(txn, map.get(txn, "stable_id"))
        .filter(|value| is_valid_stable_id(value))
        .unwrap_or_else(|| legacy_entity_stable_id("note", id));
    let group_id = number(map.get(txn, "group_id"));
    Some(Note {
        id,
        stable_id,
        text: string(txn, map.get(txn, "text")).unwrap_or_default(),
        color: parse_color(string(txn, map.get(txn, "color")).as_deref()),
        status: parse_status(string(txn, map.get(txn, "status")).as_deref()),
        due_date: string(txn, map.get(txn, "due_date")),
        x: decimal(map.get(txn, "x")).unwrap_or_default(),
        y: decimal(map.get(txn, "y")).unwrap_or_default(),
        rotation: signed_i8(map.get(txn, "rotation")).unwrap_or_default(),
        group_id,
        group_stable_id: string(txn, map.get(txn, "group_stable_id"))
            .filter(|value| is_valid_stable_id(value))
            .or_else(|| group_id.map(|id| legacy_entity_stable_id("group", id))),
        created_at: number(map.get(txn, "created_at")).unwrap_or_default(),
        updated_at: number(map.get(txn, "updated_at")).unwrap_or_default(),
        deleted_at: number(map.get(txn, "deleted_at")),
    })
}

fn map_to_group<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Group> {
    let group = read_group(txn, key, map)?;
    (!entity_is_deleted(txn, map, group.deleted_at)).then_some(group)
}

fn read_group<T: ReadTxn>(txn: &T, key: &str, map: &MapRef) -> Option<Group> {
    let id = number(map.get(txn, "id")).or_else(|| key.parse::<u64>().ok())?;
    let stable_id = string(txn, map.get(txn, "stable_id"))
        .filter(|value| is_valid_stable_id(value))
        .unwrap_or_else(|| legacy_entity_stable_id("group", id));
    Some(Group {
        id,
        stable_id,
        label: string(txn, map.get(txn, "label")).unwrap_or_default(),
        origin: pair(map.get(txn, "origin_x"), map.get(txn, "origin_y")),
        size: pair(map.get(txn, "size_width"), map.get(txn, "size_height")),
        created_at: number(map.get(txn, "created_at")).unwrap_or_default(),
        updated_at: number(map.get(txn, "updated_at")).unwrap_or_default(),
        deleted_at: number(map.get(txn, "deleted_at")),
    })
}

fn string<T: ReadTxn>(txn: &T, value: Option<yrs::Out>) -> Option<String> {
    value.and_then(|value| match value {
        yrs::Out::Any(Any::String(value)) => Some(value.to_string()),
        yrs::Out::YText(value) => Some(value.get_string(txn)),
        _ => None,
    })
}

fn sync_text_field(txn: &mut yrs::TransactionMut, map: &MapRef, key: &str, value: &str) {
    if let Some(yrs::Out::YText(text)) = map.get(txn, key) {
        let current = text.get_string(txn);
        if current != value {
            if !current.is_empty() {
                text.remove_range(txn, 0, current.len() as u32);
            }
            if !value.is_empty() {
                text.insert(txn, 0, value);
            }
        }
        return;
    }

    // Existing v1 snapshots stored text as a scalar Y.Map value. Replacing
    // that value with Y.Text is a one-time CRDT migration; subsequent edits
    // retain character-level merge behavior.
    map.remove(txn, key);
    let text = map.insert(txn, key, TextPrelim::default());
    if !value.is_empty() {
        text.insert(txn, 0, value);
    }
}

fn sync_text_field_delta(
    txn: &mut yrs::TransactionMut,
    map: &MapRef,
    key: &str,
    previous: &str,
    next: &str,
) {
    let Some(yrs::Out::YText(text)) = map.get(txn, key) else {
        sync_text_field(txn, map, key, next);
        return;
    };
    let current = text.get_string(txn);
    let previous_chars: Vec<char> = previous.chars().collect();
    let next_chars: Vec<char> = next.chars().collect();
    let prefix = previous_chars
        .iter()
        .zip(&next_chars)
        .take_while(|(left, right)| left == right)
        .count();
    let mut suffix = 0;
    while suffix < previous_chars.len().saturating_sub(prefix)
        && suffix < next_chars.len().saturating_sub(prefix)
        && previous_chars[previous_chars.len() - 1 - suffix]
            == next_chars[next_chars.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let removed: String = previous_chars[prefix..previous_chars.len() - suffix]
        .iter()
        .collect();
    let inserted: String = next_chars[prefix..next_chars.len() - suffix]
        .iter()
        .collect();
    let current_chars: Vec<char> = current.chars().collect();
    let removed_chars: Vec<char> = removed.chars().collect();
    let prefix_indices = subsequence_indices(&current_chars, &previous_chars[..prefix]);
    let removed_indices = if removed_chars.is_empty() {
        Some(Vec::new())
    } else {
        current_chars
            .windows(removed_chars.len())
            .position(|window| window == removed_chars.as_slice())
            .map(|start| (start..start + removed_chars.len()).collect())
            .or_else(|| subsequence_indices(&current_chars, &removed_chars))
    };
    let Some(removed_indices) = removed_indices else {
        // The local projection no longer shares any recoverable anchors with
        // the current Y.Text value. A full replacement is the only honest
        // result in this rare case; normal concurrent insertions and partial
        // replacements are handled by the anchored path below.
        sync_text_field(txn, map, key, next);
        return;
    };

    // Locate the edit relative to the old projection, not simply at the same
    // numeric offset in the current text. Concurrent characters can appear
    // before or inside the old span; deleting only the characters identified
    // by the old projection preserves those remote insertions.
    let raw_start = if let Some(indices) = prefix_indices {
        indices.last().map_or(0, |index| index + 1)
    } else {
        removed_indices
            .first()
            .copied()
            .unwrap_or_else(|| prefix.min(current_chars.len()))
    };
    let removed_before_start = removed_indices
        .iter()
        .filter(|index| **index < raw_start)
        .count();
    let start = raw_start
        .saturating_sub(removed_before_start)
        .min(current_chars.len().saturating_sub(removed_indices.len()));
    for index in removed_indices.iter().rev() {
        text.remove_range(txn, *index as u32, 1);
    }
    if !inserted.is_empty() {
        text.insert(txn, start as u32, &inserted);
    }
}

fn subsequence_indices(haystack: &[char], needle: &[char]) -> Option<Vec<usize>> {
    if needle.is_empty() {
        return Some(Vec::new());
    }
    let mut next = 0;
    let mut indices = Vec::with_capacity(needle.len());
    for (index, character) in haystack.iter().enumerate() {
        if *character == needle[next] {
            indices.push(index);
            next += 1;
            if next == needle.len() {
                return Some(indices);
            }
        }
    }
    None
}

fn migrate_legacy_text_fields(txn: &mut yrs::TransactionMut) {
    for root in [ROOT_NOTES, ROOT_GROUPS] {
        let Some(collection) = txn.get(root).and_then(out_map) else {
            continue;
        };
        let keys: Vec<String> = collection
            .iter(txn)
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in keys {
            let Some(map) = collection.get(txn, &key).and_then(out_map) else {
                continue;
            };
            let field = if root == ROOT_NOTES { "text" } else { "label" };
            let Some(yrs::Out::Any(Any::String(value))) = map.get(txn, field) else {
                continue;
            };
            sync_text_field(txn, &map, field, value.as_ref());
        }
    }
}

/// Re-key legacy numeric entity maps under their canonical stable UUID.
///
/// The numeric `id` remains in every map as a compatibility alias for the
/// current UI and route format. Re-running this migration is a no-op, and
/// tombstoned maps are migrated exactly like visible maps.
fn migrate_legacy_entity_ids(txn: &mut yrs::TransactionMut) {
    for (root, kind) in [(ROOT_NOTES, "note"), (ROOT_GROUPS, "group")] {
        let Some(collection) = txn.get(root).and_then(out_map) else {
            continue;
        };
        let keys: Vec<String> = collection
            .iter(txn)
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in keys {
            let Some(source) = collection.get(txn, &key).and_then(out_map) else {
                continue;
            };
            let Some(id) = number(source.get(txn, "id")).or_else(|| key.parse::<u64>().ok()) else {
                continue;
            };
            let stable_id = string(txn, source.get(txn, "stable_id"))
                .filter(|value| is_valid_stable_id(value))
                .unwrap_or_else(|| legacy_entity_stable_id(kind, id));
            if key == stable_id {
                source.try_update(txn, "id", id as i64);
                source.try_update(txn, "stable_id", stable_id);
                if kind == "note"
                    && let Some(group_id) = number(source.get(txn, "group_id"))
                    && string(txn, source.get(txn, "group_stable_id"))
                        .is_none_or(|value| !is_valid_stable_id(&value))
                {
                    source.try_update(
                        txn,
                        "group_stable_id",
                        legacy_entity_stable_id("group", group_id),
                    );
                }
                continue;
            }

            let destination: MapRef = collection.get_or_init(txn, stable_id);
            if kind == "note" {
                if let Some(note) = read_note(txn, &key, &source) {
                    sync_note_map(txn, &destination, &note);
                }
            } else if let Some(group) = read_group(txn, &key, &source) {
                sync_group_map(txn, &destination, &group);
            }
            collection.remove(txn, &key);
        }
    }
}

/// Record the lifecycle intent separately from the compatibility
/// `deleted_at` register. A plain Yrs map register is deterministic, but a
/// concurrent restore could win that register even when it never observed a
/// delete. The lifecycle records let projection code apply delete-wins
/// semantics while still allowing a causally newer explicit restore.
fn record_delete_lifecycle(
    txn: &mut yrs::TransactionMut,
    entity: &MapRef,
    client_id: ClientID,
    deleted_at: u64,
) {
    let operation_clock = txn.state_vector().get(&client_id).saturating_add(1);
    let key = format!(
        "{DELETE_OPERATION_PREFIX}{}:{operation_clock}",
        client_id.get()
    );
    let lifecycle: MapRef = entity.get_or_init(txn, ENTITY_LIFECYCLE);
    let operation: MapRef = lifecycle.get_or_init(txn, key);
    operation.try_update(txn, "client_id", client_id.get() as i64);
    operation.try_update(txn, "clock", operation_clock as i64);
    operation.try_update(txn, "deleted_at", deleted_at as i64);
}

fn record_restore_lifecycle(txn: &mut yrs::TransactionMut, entity: &MapRef, client_id: ClientID) {
    let context = txn.state_vector().encode_v1();
    let operation_clock = txn.state_vector().get(&client_id).saturating_add(1);
    let key = format!(
        "{RESTORE_OPERATION_PREFIX}{}:{operation_clock}",
        client_id.get()
    );
    let lifecycle: MapRef = entity.get_or_init(txn, ENTITY_LIFECYCLE);
    let operation: MapRef = lifecycle.get_or_init(txn, key);
    operation.try_update(txn, "client_id", client_id.get() as i64);
    operation.try_update(txn, "clock", operation_clock as i64);
    operation.try_update(txn, "context", URL_SAFE_NO_PAD.encode(context));
}

/// Add a lifecycle anchor for a legacy scalar tombstone. The anchor is a
/// normal Yrs insertion, so a restore loaded from a state that includes the
/// tombstone can prove causal observation while a stale restore cannot.
fn migrate_legacy_lifecycle_markers(txn: &mut yrs::TransactionMut, client_id: ClientID) {
    for root in [ROOT_NOTES, ROOT_GROUPS] {
        let Some(collection) = txn.get(root).and_then(out_map) else {
            continue;
        };
        let keys: Vec<String> = collection
            .iter(txn)
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in keys {
            let Some(entity) = collection.get(txn, &key).and_then(out_map) else {
                continue;
            };
            // Materialize the lifecycle map for every entity before any
            // replica can concurrently add a delete or restore record. If
            // two replicas created this nested map at the same time, Yrs
            // would resolve the parent register and could hide one side's
            // lifecycle operation.
            let _: MapRef = entity.get_or_init(txn, ENTITY_LIFECYCLE);
            let Some(deleted_at) = number(entity.get(txn, "deleted_at")) else {
                continue;
            };
            if !has_delete_lifecycle_marker(txn, &entity) {
                record_delete_lifecycle(txn, &entity, client_id, deleted_at);
            }
        }
    }
}

fn has_delete_lifecycle_marker<T: ReadTxn>(txn: &T, entity: &MapRef) -> bool {
    entity
        .get(txn, ENTITY_LIFECYCLE)
        .and_then(out_map)
        .is_some_and(|lifecycle| {
            lifecycle
                .keys(txn)
                .any(|key| key.starts_with(DELETE_OPERATION_PREFIX))
        })
}

fn entity_is_deleted<T: ReadTxn>(txn: &T, entity: &MapRef, scalar_deleted_at: Option<u64>) -> bool {
    let Some(lifecycle) = entity.get(txn, ENTITY_LIFECYCLE).and_then(out_map) else {
        return scalar_deleted_at.is_some();
    };

    let mut deletions = Vec::new();
    let mut restores = Vec::new();
    for (key, value) in lifecycle.iter(txn) {
        let Some(operation) = out_map(value) else {
            if key.starts_with(DELETE_OPERATION_PREFIX) {
                return true;
            }
            continue;
        };
        if key.starts_with(DELETE_OPERATION_PREFIX) {
            let Some(client_id) = number(operation.get(txn, "client_id")) else {
                return true;
            };
            let Some(clock) =
                number(operation.get(txn, "clock")).and_then(|clock| u32::try_from(clock).ok())
            else {
                return true;
            };
            deletions.push((client_id, clock));
        } else if key.starts_with(RESTORE_OPERATION_PREFIX)
            && let Some(context) = string(txn, operation.get(txn, "context"))
            && let Ok(bytes) = URL_SAFE_NO_PAD.decode(context)
            && let Ok(context) = StateVector::decode_v1(&bytes)
        {
            restores.push(context);
        }
    }

    if deletions.is_empty() {
        return scalar_deleted_at.is_some();
    }
    deletions.iter().any(|(client_id, clock)| {
        !restores
            .iter()
            .any(|context| context.get(&ClientID::new(*client_id)) >= *clock)
    })
}

/// Return the newest unresolved deletion timestamp for retention decisions.
/// A restore may have won the compatibility scalar while still being stale;
/// in that case the lifecycle deletion record remains the authoritative
/// tombstone age. Invalid lifecycle records fail closed and are not compacted.
fn entity_tombstone_at<T: ReadTxn>(txn: &T, entity: &MapRef) -> Option<u64> {
    let scalar_deleted_at = number(entity.get(txn, "deleted_at"));
    let Some(lifecycle) = entity.get(txn, ENTITY_LIFECYCLE).and_then(out_map) else {
        return scalar_deleted_at;
    };

    let mut deletions = Vec::new();
    let mut restores = Vec::new();
    for (key, value) in lifecycle.iter(txn) {
        let Some(operation) = out_map(value) else {
            if key.starts_with(DELETE_OPERATION_PREFIX) {
                return None;
            }
            continue;
        };
        if key.starts_with(DELETE_OPERATION_PREFIX) {
            let client_id = number(operation.get(txn, "client_id"))?;
            let clock =
                number(operation.get(txn, "clock")).and_then(|clock| u32::try_from(clock).ok())?;
            let deleted_at = number(operation.get(txn, "deleted_at"))?;
            deletions.push((client_id, clock, deleted_at));
        } else if key.starts_with(RESTORE_OPERATION_PREFIX)
            && let Some(context) = string(txn, operation.get(txn, "context"))
            && let Ok(bytes) = URL_SAFE_NO_PAD.decode(context)
            && let Ok(context) = StateVector::decode_v1(&bytes)
        {
            restores.push(context);
        }
    }
    if deletions.is_empty() {
        return scalar_deleted_at;
    }
    deletions
        .into_iter()
        .filter(|(client_id, clock, _)| {
            !restores
                .iter()
                .any(|context| context.get(&ClientID::new(*client_id)) >= *clock)
        })
        .map(|(_, _, deleted_at)| deleted_at)
        .max()
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
            stable_id: crate::legacy_entity_stable_id("note", id),
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
    fn canonical_entity_keys_are_uuid_backed_with_numeric_aliases() {
        let board = BoardData {
            notes: vec![note(17, "stable")],
            groups: vec![crate::Group {
                id: 18,
                label: "group".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let document = SpaceDoc::new();
        document.import_board(&board);

        let transaction = document.doc().transact();
        let notes = transaction
            .get(ROOT_NOTES)
            .and_then(out_map)
            .expect("notes root should exist");
        let (key, value) = notes.iter(&transaction).next().expect("note should exist");
        uuid::Uuid::parse_str(key).expect("note map key should be a UUID");
        let note_map = out_map(value).expect("note should be a map");
        assert_eq!(number(note_map.get(&transaction, "id")), Some(17));
        assert_eq!(
            string(&transaction, note_map.get(&transaction, "stable_id")),
            Some(key.to_owned())
        );
    }

    #[test]
    fn legacy_numeric_maps_migrate_without_losing_tombstones() {
        let legacy = Doc::new();
        let mut transaction = legacy.transact_mut();
        let meta = transaction.get_or_insert_map(ROOT_META);
        meta.try_update(
            &mut transaction,
            "schema_version",
            LEGACY_ENTITY_SCHEMA_VERSION,
        );
        let notes = transaction.get_or_insert_map(ROOT_NOTES);
        let note_map: MapRef = notes.get_or_init(&mut transaction, "91");
        note_map.try_update(&mut transaction, "id", 91_i64);
        note_map.try_update(&mut transaction, "deleted_at", 123_i64);
        drop(transaction);

        let migrated = SpaceDoc::from_update(
            &legacy
                .transact()
                .encode_state_as_update_v1(&StateVector::default()),
        )
        .expect("legacy document should migrate");
        let transaction = migrated.doc().transact();
        let notes = transaction
            .get(ROOT_NOTES)
            .and_then(out_map)
            .expect("notes root should exist");
        let (key, value) = notes
            .iter(&transaction)
            .next()
            .expect("tombstone should remain");
        assert_eq!(key, crate::legacy_entity_stable_id("note", 91));
        let note_map = out_map(value).expect("tombstone should be a map");
        assert_eq!(number(note_map.get(&transaction, "deleted_at")), Some(123));
        assert!(migrated.board().notes.is_empty());
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
    fn legacy_scalar_text_is_migrated_to_ytext() {
        let legacy = Doc::new();
        let mut transaction = legacy.transact_mut();
        let meta = transaction.get_or_insert_map(ROOT_META);
        meta.try_update(
            &mut transaction,
            "schema_version",
            LEGACY_CRDT_SCHEMA_VERSION,
        );
        let notes = transaction.get_or_insert_map(ROOT_NOTES);
        let note_map: MapRef = notes.get_or_init(&mut transaction, "11");
        note_map.try_update(&mut transaction, "id", 11_i64);
        note_map.try_update(&mut transaction, "text", "legacy scalar");
        drop(transaction);

        let migrated = SpaceDoc::from_update(
            &legacy
                .transact()
                .encode_state_as_update_v1(&StateVector::default()),
        )
        .expect("legacy snapshot should migrate");
        assert_eq!(migrated.schema_version(), CRDT_SCHEMA_VERSION);
        assert_eq!(migrated.board().notes[0].text, "legacy scalar");

        let transaction = migrated.doc().transact();
        let notes = transaction
            .get(ROOT_NOTES)
            .and_then(out_map)
            .expect("notes root should exist");
        let note_map = notes
            .get(&transaction, &crate::legacy_entity_stable_id("note", 11))
            .and_then(out_map)
            .expect("note should exist");
        assert!(matches!(
            note_map.get(&transaction, "text"),
            Some(Out::YText(_))
        ));
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
    fn persisted_snapshot_merge_preserves_cached_local_operations() {
        let base = SpaceDoc::new();
        base.import_board(&BoardData {
            notes: vec![note(12, "base")],
            ..Default::default()
        });
        let cached = SpaceDoc::from_update(&base.snapshot()).unwrap();
        let persisted = SpaceDoc::from_update(&base.snapshot()).unwrap();

        cached.set_note_field(12, "text", Any::from("cached edit"));
        persisted.set_note_field(12, "due_date", Any::from("2026-09-12"));

        cached
            .apply_snapshot(&persisted.snapshot())
            .expect("persisted snapshot should merge into cached document");

        let merged = cached.board().notes[0].clone();
        assert_eq!(merged.text, "cached edit");
        assert_eq!(merged.due_date.as_deref(), Some("2026-09-12"));
    }

    #[test]
    fn tombstones_survive_until_explicit_retention_compaction() {
        let document = SpaceDoc::new();
        document.import_board(&BoardData {
            notes: vec![note(13, "retained")],
            ..Default::default()
        });
        assert!(document.delete_note(13, 100));
        assert!(document.board().notes.is_empty());

        assert!(!document.compact_tombstones(99));
        let retained = SpaceDoc::from_update(&document.snapshot()).unwrap();
        assert!(retained.board().notes.is_empty());

        assert!(document.compact_tombstones(100));
        let compacted = SpaceDoc::from_update(&document.snapshot()).unwrap();
        assert!(compacted.board().notes.is_empty());
    }

    #[test]
    fn stale_snapshot_cannot_resurrect_a_compacted_tombstone() {
        let document = SpaceDoc::new();
        document.import_board(&BoardData {
            notes: vec![note(14, "will delete")],
            ..Default::default()
        });
        let stale_snapshot = document.snapshot();
        assert!(document.delete_note(14, 100));
        assert!(document.compact_tombstones(100));

        let compacted = SpaceDoc::from_update(&document.snapshot()).unwrap();
        compacted
            .apply_update(&stale_snapshot)
            .expect("stale snapshot should remain a valid update");
        assert!(compacted.board().notes.is_empty());
    }

    #[test]
    fn stale_restore_does_not_prevent_tombstone_compaction() {
        let first = SpaceDoc::new();
        first.import_board(&BoardData {
            notes: vec![note(15, "compact me")],
            ..Default::default()
        });
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        assert!(first.delete_note(15, 100));
        assert!(second.restore_note(15));
        let delete_update = first.encode_update(&state).unwrap();
        let restore_update = second.encode_update(&state).unwrap();
        first.apply_update(&restore_update).unwrap();
        second.apply_update(&delete_update).unwrap();
        assert!(first.board().notes.is_empty());

        assert!(first.compact_tombstones(100));
        let compacted = SpaceDoc::from_update(&first.snapshot()).unwrap();
        assert!(compacted.board().notes.is_empty());
    }

    #[test]
    fn concurrent_projection_text_splices_preserve_both_insertions() {
        let base_board = BoardData {
            notes: vec![note(5, "abc")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        let mut first_board = base_board.clone();
        first_board.notes[0].text = "aXbc".into();
        first.apply_board_diff(&base_board, &first_board, 1);
        let mut second_board = base_board.clone();
        second_board.notes[0].text = "abYc".into();
        second.apply_board_diff(&base_board, &second_board, 1);

        let first_update = first.encode_update(&state).unwrap();
        let second_update = second.encode_update(&state).unwrap();
        first.apply_update(&second_update).unwrap();
        second.apply_update(&first_update).unwrap();
        assert_eq!(first.board(), second.board());
        assert!(first.board().notes[0].text.contains('X'));
        assert!(first.board().notes[0].text.contains('Y'));
    }

    #[test]
    fn stale_full_text_replacement_preserves_concurrent_insertions() {
        let base_board = BoardData {
            notes: vec![note(6, "abc")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        let mut first_board = base_board.clone();
        first_board.notes[0].text = "xyz".into();
        first.apply_board_diff(&base_board, &first_board, 1);

        let mut second_board = base_board.clone();
        second_board.notes[0].text = "aYbc".into();
        second.apply_board_diff(&base_board, &second_board, 1);

        let first_update = first.encode_update(&state).unwrap();
        let second_update = second.encode_update(&state).unwrap();
        first.apply_update(&second_update).unwrap();
        second.apply_update(&first_update).unwrap();

        assert_eq!(first.board(), second.board());
        let text = &first.board().notes[0].text;
        assert!(text.contains("xyz"));
        assert!(text.contains('Y'));
    }

    #[test]
    fn concurrent_text_inserts_at_the_same_position_preserve_both() {
        let base_board = BoardData {
            notes: vec![note(7, "abc")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        let mut first_board = base_board.clone();
        first_board.notes[0].text = "aXbc".into();
        first.apply_board_diff(&base_board, &first_board, 1);
        let mut second_board = base_board.clone();
        second_board.notes[0].text = "aYbc".into();
        second.apply_board_diff(&base_board, &second_board, 1);

        let first_update = first.encode_update(&state).unwrap();
        let second_update = second.encode_update(&state).unwrap();
        first.apply_update(&second_update).unwrap();
        second.apply_update(&first_update).unwrap();

        assert_eq!(first.board(), second.board());
        let text = &first.board().notes[0].text;
        assert!(text.contains('X'));
        assert!(text.contains('Y'));
    }

    #[test]
    fn concurrent_same_scalar_edits_converge_without_wall_clock_ordering() {
        let base_board = BoardData {
            notes: vec![note(9, "scalar")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        first.set_note_field(9, "color", Any::from("pink"));
        second.set_note_field(9, "color", Any::from("blue"));

        let first_update = first.encode_update(&state).unwrap();
        let second_update = second.encode_update(&state).unwrap();
        first.apply_update(&second_update).unwrap();
        second.apply_update(&first_update).unwrap();

        assert_eq!(first.board(), second.board());
        assert!(matches!(
            first.board().notes[0].color,
            NoteColor::Pink | NoteColor::Blue
        ));
    }

    #[test]
    fn concurrent_edit_and_delete_keep_the_tombstone_winner() {
        let base_board = BoardData {
            notes: vec![note(10, "delete me")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
        let state = first.state_vector();

        first.delete_note(10, 2);
        second.set_note_field(10, "text", Any::from("late edit"));

        let first_update = first.encode_update(&state).unwrap();
        let second_update = second.encode_update(&state).unwrap();
        first.apply_update(&second_update).unwrap();
        second.apply_update(&first_update).unwrap();

        assert_eq!(first.board(), second.board());
        assert!(first.board().notes.is_empty());
    }

    #[test]
    fn causally_newer_restore_can_explicitly_revive_a_deleted_note() {
        let base_board = BoardData {
            notes: vec![note(11, "restore me")],
            ..Default::default()
        };
        let first = SpaceDoc::new();
        first.import_board(&base_board);
        let second = SpaceDoc::from_update(&first.snapshot()).unwrap();

        assert!(first.delete_note(11, 3));
        let delete_update = first
            .encode_update(&second.state_vector())
            .expect("delete update should encode");
        second
            .apply_update(&delete_update)
            .expect("delete update should apply");
        assert!(second.board().notes.is_empty());

        assert!(second.restore_note(11));
        let restore_update = second
            .encode_update(&first.state_vector())
            .expect("restore update should encode");
        first
            .apply_update(&restore_update)
            .expect("restore update should apply");

        assert_eq!(first.board(), second.board());
        assert_eq!(first.board().notes[0].text, "restore me");
    }

    #[test]
    fn concurrent_stale_restore_does_not_beat_a_delete() {
        for _ in 0..128 {
            let base_board = BoardData {
                notes: vec![note(12, "delete wins")],
                ..Default::default()
            };
            let first = SpaceDoc::new();
            first.import_board(&base_board);
            let second = SpaceDoc::from_update(&first.snapshot()).unwrap();
            let state = first.state_vector();

            assert!(first.delete_note(12, 4));
            assert!(second.restore_note(12));

            let delete_update = first.encode_update(&state).unwrap();
            let restore_update = second.encode_update(&state).unwrap();
            first.apply_update(&restore_update).unwrap();
            second.apply_update(&delete_update).unwrap();

            assert_eq!(first.board(), second.board());
            assert!(first.board().notes.is_empty());
        }
    }

    #[test]
    fn three_replicas_converge_with_duplicate_and_reordered_updates() {
        let base_board = BoardData {
            notes: vec![note(8, "base")],
            groups: vec![crate::Group {
                id: 21,
                label: "work".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let base = SpaceDoc::new();
        base.import_board(&base_board);
        let state = base.state_vector();
        let replicas = (0..3)
            .map(|index| {
                let replica = SpaceDoc::from_update(&base.snapshot()).unwrap();
                match index {
                    0 => replica.set_note_field(8, "text", "alpha".into()),
                    1 => replica.set_note_field(8, "due_date", "2026-09-11".into()),
                    _ => replica.set_note_field(8, "group_id", Any::from(21_i64)),
                };
                replica
            })
            .collect::<Vec<_>>();
        let updates = replicas
            .iter()
            .map(|replica| replica.encode_update(&state).unwrap())
            .collect::<Vec<_>>();

        let first = SpaceDoc::from_update(&base.snapshot()).unwrap();
        let second = SpaceDoc::from_update(&base.snapshot()).unwrap();
        let third = SpaceDoc::from_update(&base.snapshot()).unwrap();
        for update in [&updates[2], &updates[0], &updates[2], &updates[1]] {
            first.apply_update(update).unwrap();
        }
        for update in [&updates[1], &updates[2], &updates[0], &updates[1]] {
            second.apply_update(update).unwrap();
        }
        for update in [&updates[0], &updates[1], &updates[2], &updates[0]] {
            third.apply_update(update).unwrap();
        }

        assert_eq!(first.board(), second.board());
        assert_eq!(second.board(), third.board());
        assert_eq!(first.board().notes[0].text, "alpha");
        assert_eq!(
            first.board().notes[0].due_date.as_deref(),
            Some("2026-09-11")
        );
        assert_eq!(first.board().notes[0].group_id, Some(21));
    }

    #[test]
    fn many_replicas_converge_after_duplicate_reordered_delivery() {
        let base_board = BoardData {
            notes: vec![note(40, "base")],
            ..Default::default()
        };
        let base = SpaceDoc::new();
        base.import_board(&base_board);
        let state = base.state_vector();
        let updates = (0..8)
            .map(|index| {
                let replica = SpaceDoc::from_update(&base.snapshot()).unwrap();
                let mut board = base_board.clone();
                board
                    .notes
                    .push(note(100 + index, &format!("replica-{index}")));
                replica.apply_board_diff(&base_board, &board, index + 1);
                replica.encode_update(&state).unwrap()
            })
            .collect::<Vec<_>>();

        let delivery_orders = [
            [0, 1, 2, 3, 4, 5, 6, 7, 0, 3],
            [7, 6, 5, 4, 3, 2, 1, 0, 7, 2],
            [3, 0, 6, 1, 7, 4, 2, 5, 1, 6],
            [5, 2, 7, 0, 4, 1, 6, 3, 5, 0],
        ];
        let replicas = delivery_orders
            .iter()
            .map(|order| {
                let replica = SpaceDoc::from_update(&base.snapshot()).unwrap();
                for index in order {
                    replica.apply_update(&updates[*index]).unwrap();
                }
                replica
            })
            .collect::<Vec<_>>();

        for replica in replicas.iter().skip(1) {
            assert_eq!(replica.board(), replicas[0].board());
        }
        assert_eq!(replicas[0].board().notes.len(), 9);
    }

    #[test]
    fn randomized_delivery_orders_converge_across_replicas() {
        let base_board = BoardData {
            notes: vec![note(50, "base")],
            ..Default::default()
        };
        let base = SpaceDoc::new();
        base.import_board(&base_board);
        let state = base.state_vector();
        let updates = (0..7)
            .map(|index| {
                let replica = SpaceDoc::from_update(&base.snapshot()).unwrap();
                let mut board = base_board.clone();
                board
                    .notes
                    .push(note(200 + index, &format!("replica-{index}")));
                replica.apply_board_diff(&base_board, &board, 1_000 + index);
                replica.encode_update(&state).unwrap()
            })
            .collect::<Vec<_>>();

        // A small deterministic PRNG keeps this property-style test
        // dependency-free while exercising many delivery schedules in every
        // normal workspace test run.
        let mut seed = 0x5eed_u64;
        for _trial in 0..64 {
            let mut order = (0..updates.len()).collect::<Vec<_>>();
            order.extend([0, 3, 6, 1]);
            for index in (1..order.len()).rev() {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let swap_with = (seed as usize) % (index + 1);
                order.swap(index, swap_with);
            }

            let first = SpaceDoc::from_update(&base.snapshot()).unwrap();
            let second = SpaceDoc::from_update(&base.snapshot()).unwrap();
            let third = SpaceDoc::from_update(&base.snapshot()).unwrap();
            for index in &order {
                first.apply_update(&updates[*index]).unwrap();
            }
            for index in order.iter().rev() {
                second.apply_update(&updates[*index]).unwrap();
            }
            for index in order.iter().skip(2).chain(order.iter().take(2)) {
                third.apply_update(&updates[*index]).unwrap();
            }

            assert_eq!(first.board(), second.board());
            assert_eq!(second.board(), third.board());
            assert_eq!(first.board().notes.len(), 8);
        }
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
    fn stale_projection_import_cannot_resurrect_deleted_entities() {
        let doc = SpaceDoc::new();
        doc.import_board(&BoardData {
            notes: vec![note(3, "recoverable")],
            tombstones: vec![crate::Tombstone {
                kind: crate::TombstoneKind::Note,
                id: 3,
                stable_id: crate::legacy_entity_stable_id("note", 3),
                deleted_at: 100,
            }],
            ..Default::default()
        });
        assert!(doc.board().notes.is_empty());

        doc.import_board(&BoardData {
            notes: vec![note(3, "recoverable")],
            ..Default::default()
        });
        assert!(doc.board().notes.is_empty());
        assert!(doc.restore_note(3));
        assert_eq!(doc.board().notes[0].text, "recoverable");
    }

    #[test]
    fn projection_diff_updates_only_changed_fields_and_tombstones_removals() {
        let doc = SpaceDoc::new();
        let before = BoardData {
            notes: vec![note(4, "before")],
            ..Default::default()
        };
        doc.import_board(&before);

        let mut after = before.clone();
        after.notes[0].text = "after".into();
        doc.apply_board_diff(&before, &after, 123);
        assert_eq!(doc.board().notes[0].text, "after");

        doc.apply_board_diff(&after, &BoardData::default(), 456);
        assert!(doc.board().notes.is_empty());
        assert!(doc.restore_note(4));
        assert_eq!(doc.board().notes[0].text, "after");
    }
}
