use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::Value;
use temps_agent_runtime::{
    ChatQueueStore as DurableChatQueueStore, QueuedChatMessage, QueuedChatMessagePage,
};
use tokio_rusqlite::rusqlite::{params, OptionalExtension};

use crate::{
    now_ms, ApprovalRequest, ChatEvent, ChatMessage, ChatSnapshot, ChatStatus, ChatView,
    SshConnectionSummary, StoredSshAuthentication, StoredSshConnection,
};

const MAX_CHATS: i64 = 1_000;
const MAX_EVENTS_PER_CHAT: u64 = 2_000;
const MAX_MESSAGES_PER_CHAT: u64 = 1_000;

pub(crate) type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite operation failed: {0}")]
    Sqlite(#[from] tokio_rusqlite::Error),
    #[error("could not open SQLite database: {0}")]
    Open(#[from] tokio_rusqlite::rusqlite::Error),
    #[error("stored JSON could not be encoded or decoded: {0}")]
    Json(#[from] serde_json::Error),
    #[error("SSH connection secret operation failed: {0}")]
    Crypto(String),
    #[error("chat `{0}` does not exist")]
    ChatNotFound(String),
    #[error("queued message `{0}` does not exist")]
    QueueMessageNotFound(String),
    #[error("queued message `{0}` changed before this update")]
    QueueConflict(String),
}

#[async_trait]
pub(crate) trait ChatStore: Send + Sync {
    async fn allocate_chat_id(&self) -> StoreResult<String>;
    async fn allocate_queued_message_id(&self) -> StoreResult<String>;
    async fn allocate_ssh_connection_id(&self) -> StoreResult<String>;
    async fn list_ssh_connections(&self) -> StoreResult<Vec<SshConnectionSummary>>;
    async fn load_ssh_connection(&self, id: &str) -> StoreResult<Option<StoredSshConnection>>;
    async fn save_ssh_connection(&self, connection: &StoredSshConnection) -> StoreResult<()>;
    async fn delete_ssh_connection(&self, id: &str) -> StoreResult<bool>;
    async fn create_chat(
        &self,
        snapshot: &ChatSnapshot,
        initial_message: &ChatMessage,
    ) -> StoreResult<()>;
    async fn list_chats(
        &self,
        connection_key: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<ChatSnapshot>>;
    async fn load_chat(&self, chat_id: &str) -> StoreResult<Option<ChatView>>;
    async fn save_snapshot_and_event(
        &self,
        snapshot: &ChatSnapshot,
        event: &ChatEvent,
    ) -> StoreResult<()>;
    async fn save_snapshot_message_and_event(
        &self,
        snapshot: &ChatSnapshot,
        message: &ChatMessage,
        event: &ChatEvent,
    ) -> StoreResult<()>;
    async fn save_approval(
        &self,
        chat_id: &str,
        message_sequence: u64,
        request: &ApprovalRequest,
    ) -> StoreResult<()>;
    async fn resolve_approval(
        &self,
        chat_id: &str,
        message_sequence: u64,
        approval_id: &str,
        decision: &str,
    ) -> StoreResult<()>;
    async fn recover_interrupted_chats(&self) -> StoreResult<usize>;
}

#[derive(Clone)]
pub(crate) struct SqliteChatStore {
    connection: tokio_rusqlite::Connection,
    ssh_secret_key: [u8; 32],
}

fn random_bytes<const N: usize>() -> StoreResult<[u8; N]> {
    let mut bytes = [0_u8; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| StoreError::Crypto("the operating system random generator failed".into()))?;
    Ok(bytes)
}

fn load_or_create_secret_key(database_path: &Path) -> StoreResult<[u8; 32]> {
    if database_path == Path::new(":memory:") {
        return random_bytes();
    }
    let key_path = std::env::var_os("AGENT_RUNTIME_EXAMPLE_SSH_KEY")
        .map(PathBuf::from)
        .unwrap_or_else(|| database_path.with_extension("ssh-key"));
    if key_path.exists() {
        let mut key = [0_u8; 32];
        let mut file = std::fs::File::open(&key_path)?;
        file.read_exact(&mut key)?;
        if file.read(&mut [0_u8; 1])? != 0 {
            return Err(StoreError::Crypto(format!(
                "{} must contain exactly 32 bytes",
                key_path.display()
            )));
        }
        return Ok(key);
    }
    if let Some(parent) = key_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let key = random_bytes()?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&key_path) {
        Ok(mut file) => file.write_all(&key)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return load_or_create_secret_key(database_path);
        }
        Err(error) => return Err(error.into()),
    }
    Ok(key)
}

fn encrypt_secret(key: &[u8; 32], secret: &str) -> StoreResult<Vec<u8>> {
    let nonce_bytes = random_bytes::<12>()?;
    let key =
        LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| {
            StoreError::Crypto("could not initialize SSH secret encryption".into())
        })?);
    let mut encrypted = secret.as_bytes().to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce_bytes),
        Aad::empty(),
        &mut encrypted,
    )
    .map_err(|_| StoreError::Crypto("could not encrypt the SSH password".into()))?;
    let mut value = nonce_bytes.to_vec();
    value.extend(encrypted);
    Ok(value)
}

fn decrypt_secret(key: &[u8; 32], encrypted: &[u8]) -> StoreResult<String> {
    if encrypted.len() < 12 {
        return Err(StoreError::Crypto(
            "stored SSH password is truncated".into(),
        ));
    }
    let (nonce, ciphertext) = encrypted.split_at(12);
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| StoreError::Crypto("stored SSH password nonce is invalid".into()))?;
    let key =
        LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| {
            StoreError::Crypto("could not initialize SSH secret decryption".into())
        })?);
    let mut plaintext = ciphertext.to_vec();
    let plaintext = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::empty(),
            &mut plaintext,
        )
        .map_err(|_| StoreError::Crypto("stored SSH password could not be decrypted".into()))?;
    String::from_utf8(plaintext.to_vec())
        .map_err(|_| StoreError::Crypto("stored SSH password is not UTF-8".into()))
}

impl SqliteChatStore {
    pub(crate) async fn open(path: &Path) -> StoreResult<Self> {
        let ssh_secret_key = load_or_create_secret_key(path)?;
        let connection = if path == Path::new(":memory:") {
            tokio_rusqlite::Connection::open_in_memory().await?
        } else {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio_rusqlite::Connection::open(path).await?
        };
        connection
            .call(|database| {
                database.busy_timeout(std::time::Duration::from_secs(5))?;
                database.execute_batch(
                    "PRAGMA foreign_keys = ON;
                     PRAGMA journal_mode = WAL;
                     PRAGMA synchronous = NORMAL;
                     CREATE TABLE IF NOT EXISTS metadata (
                       key TEXT PRIMARY KEY,
                       value INTEGER NOT NULL
                     );
                     INSERT OR IGNORE INTO metadata(key, value) VALUES ('chat_sequence', 0);
                     INSERT OR IGNORE INTO metadata(key, value) VALUES ('queue_sequence', 0);
                     INSERT OR IGNORE INTO metadata(key, value) VALUES ('ssh_connection_sequence', 0);
                     CREATE TABLE IF NOT EXISTS chats (
                       id TEXT PRIMARY KEY,
                       snapshot_json TEXT NOT NULL,
                       connection_key TEXT NOT NULL DEFAULT 'local',
                       status TEXT NOT NULL,
                       created_at_ms INTEGER NOT NULL,
                       updated_at_ms INTEGER NOT NULL
                     );
                     CREATE INDEX IF NOT EXISTS chats_updated_idx
                       ON chats(updated_at_ms DESC);
                     CREATE INDEX IF NOT EXISTS chats_status_updated_idx
                       ON chats(status, updated_at_ms DESC);
                     CREATE TABLE IF NOT EXISTS chat_messages (
                       chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
                       sequence INTEGER NOT NULL,
                       role TEXT NOT NULL,
                       content TEXT NOT NULL,
                       attachments_json TEXT NOT NULL DEFAULT '[]',
                       created_at_ms INTEGER NOT NULL,
                       PRIMARY KEY(chat_id, sequence)
                     );
                     CREATE INDEX IF NOT EXISTS chat_messages_time_idx
                       ON chat_messages(chat_id, created_at_ms);
                     CREATE TABLE IF NOT EXISTS chat_events (
                       chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
                       sequence INTEGER NOT NULL,
                       timestamp_ms INTEGER NOT NULL,
                       kind TEXT NOT NULL,
                       payload_json TEXT NOT NULL,
                       PRIMARY KEY(chat_id, sequence)
                     );
                     CREATE INDEX IF NOT EXISTS chat_events_time_idx
                       ON chat_events(chat_id, timestamp_ms);
                     CREATE TABLE IF NOT EXISTS chat_turn_approvals (
                       chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
                       message_sequence INTEGER NOT NULL,
                       approval_id TEXT NOT NULL,
                       request_json TEXT NOT NULL,
                       decision TEXT,
                       created_at_ms INTEGER NOT NULL,
                       updated_at_ms INTEGER NOT NULL,
                       PRIMARY KEY(chat_id, message_sequence, approval_id)
                     );
                     CREATE INDEX IF NOT EXISTS chat_turn_approvals_pending_idx
                       ON chat_turn_approvals(chat_id, message_sequence, decision)
                       WHERE decision IS NULL;
                     CREATE TABLE IF NOT EXISTS chat_message_queue (
                       id TEXT PRIMARY KEY,
                       chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
                       content TEXT NOT NULL,
                       attachments_json TEXT NOT NULL,
                       revision INTEGER NOT NULL,
                       metadata_json TEXT NOT NULL,
                       created_at_ms INTEGER NOT NULL,
                       updated_at_ms INTEGER NOT NULL
                     );
                     CREATE INDEX IF NOT EXISTS chat_message_queue_order_idx
                       ON chat_message_queue(chat_id, created_at_ms, id);
                     CREATE TABLE IF NOT EXISTS ssh_connections (
                       id TEXT PRIMARY KEY,
                       label TEXT NOT NULL,
                       host TEXT NOT NULL,
                       user TEXT,
                       port INTEGER,
                       authentication TEXT NOT NULL,
                       identity_file TEXT,
                       password_encrypted BLOB,
                       known_hosts_file TEXT,
                       accept_new_host_key INTEGER NOT NULL,
                       created_at_ms INTEGER NOT NULL,
                       updated_at_ms INTEGER NOT NULL
                     );
                     CREATE INDEX IF NOT EXISTS ssh_connections_updated_idx
                       ON ssh_connections(updated_at_ms DESC);",
                )?;
                let has_attachments = database
                    .prepare("PRAGMA table_info(chat_messages)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()?
                    .iter()
                    .any(|column| column == "attachments_json");
                if !has_attachments {
                    database.execute(
                        "ALTER TABLE chat_messages ADD COLUMN attachments_json TEXT NOT NULL DEFAULT '[]'",
                        [],
                    )?;
                }
                let chat_columns = database
                    .prepare("PRAGMA table_info(chats)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()?;
                if !chat_columns.iter().any(|column| column == "connection_key") {
                    database.execute(
                        "ALTER TABLE chats ADD COLUMN connection_key TEXT NOT NULL DEFAULT 'local'",
                        [],
                    )?;
                    database.execute(
                        "UPDATE chats
                         SET connection_key = COALESCE(NULLIF(json_extract(snapshot_json, '$.connection_key'), ''), 'local')",
                        [],
                    )?;
                }
                database.execute(
                    "CREATE INDEX IF NOT EXISTS chats_connection_updated_idx
                     ON chats(connection_key, updated_at_ms DESC)",
                    [],
                )?;
                Ok(())
            })
            .await?;
        Ok(Self {
            connection,
            ssh_secret_key,
        })
    }

    async fn messages(&self, chat_id: &str) -> StoreResult<Vec<ChatMessage>> {
        let chat_id = chat_id.to_string();
        let rows = self
            .connection
            .call(move |database| {
                let mut statement = database.prepare(
                    "SELECT sequence, role, content, attachments_json, created_at_ms
                     FROM chat_messages WHERE chat_id = ?1
                     ORDER BY sequence ASC LIMIT ?2",
                )?;
                let rows = statement
                    .query_map(params![chat_id, MAX_MESSAGES_PER_CHAT], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, u64>(4)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        rows.into_iter()
            .map(|(sequence, role, content, attachments, created_at_ms)| {
                Ok(ChatMessage {
                    sequence,
                    role,
                    content,
                    attachments: serde_json::from_str(&attachments)?,
                    created_at_ms,
                })
            })
            .collect()
    }

    async fn events_after(&self, chat_id: &str, after: u64) -> StoreResult<Vec<ChatEvent>> {
        let chat_id = chat_id.to_string();
        let rows = self
            .connection
            .call(move |database| {
                let mut statement = database.prepare(
                    "SELECT sequence, timestamp_ms, kind, payload_json
                     FROM chat_events WHERE chat_id = ?1 AND sequence > ?2
                     ORDER BY sequence ASC LIMIT ?3",
                )?;
                let rows = statement
                    .query_map(params![chat_id, after, MAX_EVENTS_PER_CHAT], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        rows.into_iter()
            .map(|(sequence, timestamp_ms, kind, payload)| {
                Ok(ChatEvent {
                    sequence,
                    timestamp_ms,
                    kind,
                    payload: serde_json::from_str(&payload)?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl ChatStore for SqliteChatStore {
    async fn allocate_chat_id(&self) -> StoreResult<String> {
        let next = self
            .connection
            .call(|database| {
                database.execute(
                    "UPDATE metadata SET value = value + 1 WHERE key = 'chat_sequence'",
                    [],
                )?;
                database.query_row(
                    "SELECT value FROM metadata WHERE key = 'chat_sequence'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
            })
            .await?;
        Ok(format!("chat_{next:04}"))
    }

    async fn allocate_queued_message_id(&self) -> StoreResult<String> {
        let next = self
            .connection
            .call(|database| {
                database.execute(
                    "UPDATE metadata SET value = value + 1 WHERE key = 'queue_sequence'",
                    [],
                )?;
                database.query_row(
                    "SELECT value FROM metadata WHERE key = 'queue_sequence'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
            })
            .await?;
        Ok(format!("queued_{next:06}"))
    }

    async fn allocate_ssh_connection_id(&self) -> StoreResult<String> {
        let next = self
            .connection
            .call(|database| {
                database.execute(
                    "UPDATE metadata SET value = value + 1 WHERE key = 'ssh_connection_sequence'",
                    [],
                )?;
                database.query_row(
                    "SELECT value FROM metadata WHERE key = 'ssh_connection_sequence'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
            })
            .await?;
        Ok(format!("ssh_{next:04}"))
    }

    async fn list_ssh_connections(&self) -> StoreResult<Vec<SshConnectionSummary>> {
        let rows = self
            .connection
            .call(|database| {
                let mut statement = database.prepare(
                    "SELECT id, label, host, user, port, authentication, identity_file,
                            password_encrypted IS NOT NULL, known_hosts_file,
                            accept_new_host_key, created_at_ms, updated_at_ms
                     FROM ssh_connections ORDER BY updated_at_ms DESC LIMIT 200",
                )?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<u16>>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, bool>(7)?,
                            row.get::<_, Option<String>>(8)?,
                            row.get::<_, bool>(9)?,
                            row.get::<_, u64>(10)?,
                            row.get::<_, u64>(11)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        rows.into_iter()
            .map(
                |(
                    id,
                    label,
                    host,
                    user,
                    port,
                    authentication,
                    identity_file,
                    has_password,
                    known_hosts_file,
                    accept_new_host_key,
                    created_at_ms,
                    updated_at_ms,
                )| {
                    Ok(SshConnectionSummary {
                        id,
                        label,
                        host,
                        user,
                        port,
                        authentication: parse_ssh_authentication(&authentication)?,
                        identity_file: identity_file.map(PathBuf::from),
                        known_hosts_file: known_hosts_file.map(PathBuf::from),
                        accept_new_host_key,
                        has_password,
                        created_at_ms,
                        updated_at_ms,
                    })
                },
            )
            .collect()
    }

    async fn load_ssh_connection(&self, id: &str) -> StoreResult<Option<StoredSshConnection>> {
        let id = id.to_string();
        let key = self.ssh_secret_key;
        let row = self
            .connection
            .call(move |database| {
                database
                    .query_row(
                        "SELECT id, label, host, user, port, authentication, identity_file,
                                password_encrypted, known_hosts_file, accept_new_host_key,
                                created_at_ms, updated_at_ms
                         FROM ssh_connections WHERE id = ?1",
                        [id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, Option<String>>(3)?,
                                row.get::<_, Option<u16>>(4)?,
                                row.get::<_, String>(5)?,
                                row.get::<_, Option<String>>(6)?,
                                row.get::<_, Option<Vec<u8>>>(7)?,
                                row.get::<_, Option<String>>(8)?,
                                row.get::<_, bool>(9)?,
                                row.get::<_, u64>(10)?,
                                row.get::<_, u64>(11)?,
                            ))
                        },
                    )
                    .optional()
            })
            .await?;
        row.map(
            |(
                id,
                label,
                host,
                user,
                port,
                authentication,
                identity_file,
                encrypted_password,
                known_hosts_file,
                accept_new_host_key,
                created_at_ms,
                updated_at_ms,
            )| {
                Ok(StoredSshConnection {
                    id,
                    label,
                    host,
                    user,
                    port,
                    authentication: parse_ssh_authentication(&authentication)?,
                    identity_file: identity_file.map(PathBuf::from),
                    password: encrypted_password
                        .as_deref()
                        .map(|value| decrypt_secret(&key, value))
                        .transpose()?,
                    known_hosts_file: known_hosts_file.map(PathBuf::from),
                    accept_new_host_key,
                    created_at_ms,
                    updated_at_ms,
                })
            },
        )
        .transpose()
    }

    async fn save_ssh_connection(&self, connection: &StoredSshConnection) -> StoreResult<()> {
        let connection = connection.clone();
        let encrypted_password = connection
            .password
            .as_deref()
            .map(|password| encrypt_secret(&self.ssh_secret_key, password))
            .transpose()?;
        let identity_file = connection
            .identity_file
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());
        let known_hosts_file = connection
            .known_hosts_file
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());
        self.connection
            .call(move |database| {
                database.execute(
                    "INSERT INTO ssh_connections(
                       id, label, host, user, port, authentication, identity_file,
                       password_encrypted, known_hosts_file, accept_new_host_key,
                       created_at_ms, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                     ON CONFLICT(id) DO UPDATE SET
                       label = excluded.label,
                       host = excluded.host,
                       user = excluded.user,
                       port = excluded.port,
                       authentication = excluded.authentication,
                       identity_file = excluded.identity_file,
                       password_encrypted = COALESCE(excluded.password_encrypted, ssh_connections.password_encrypted),
                       known_hosts_file = excluded.known_hosts_file,
                       accept_new_host_key = excluded.accept_new_host_key,
                       updated_at_ms = excluded.updated_at_ms",
                    params![
                        connection.id,
                        connection.label,
                        connection.host,
                        connection.user,
                        connection.port,
                        ssh_authentication_name(connection.authentication),
                        identity_file,
                        encrypted_password,
                        known_hosts_file,
                        connection.accept_new_host_key,
                        connection.created_at_ms,
                        connection.updated_at_ms,
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn delete_ssh_connection(&self, id: &str) -> StoreResult<bool> {
        let id = id.to_string();
        Ok(self
            .connection
            .call(move |database| {
                database.execute("DELETE FROM ssh_connections WHERE id = ?1", [id])
            })
            .await?
            > 0)
    }

    async fn create_chat(
        &self,
        snapshot: &ChatSnapshot,
        initial_message: &ChatMessage,
    ) -> StoreResult<()> {
        let snapshot_json = serde_json::to_string(snapshot)?;
        let snapshot = snapshot.clone();
        let message = initial_message.clone();
        let attachments_json = serde_json::to_string(&message.attachments)?;
        self.connection
            .call(move |database| {
                let transaction = database.transaction()?;
                transaction.execute(
                    "INSERT INTO chats(id, snapshot_json, connection_key, status, created_at_ms, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        snapshot.id,
                        snapshot_json,
                        snapshot.connection_key,
                        status_name(snapshot.status),
                        snapshot.created_at_ms,
                        snapshot.updated_at_ms
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO chat_messages(chat_id, sequence, role, content, attachments_json, created_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        snapshot.id,
                        message.sequence,
                        message.role,
                        message.content,
                        attachments_json,
                        message.created_at_ms
                    ],
                )?;
                transaction.execute(
                    "DELETE FROM chats WHERE id IN (
                       SELECT id FROM chats WHERE status IN ('succeeded', 'failed', 'cancelled')
                       ORDER BY updated_at_ms DESC LIMIT -1 OFFSET ?1
                     )",
                    [MAX_CHATS],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn list_chats(
        &self,
        connection_key: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<ChatSnapshot>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX).min(MAX_CHATS);
        let connection_key = connection_key.map(str::to_owned);
        let rows = self
            .connection
            .call(move |database| {
                let rows = if let Some(connection_key) = connection_key {
                    let mut statement = database.prepare(
                        "SELECT snapshot_json FROM chats
                         WHERE connection_key = ?1
                         ORDER BY updated_at_ms DESC LIMIT ?2",
                    )?;
                    let mapped = statement
                        .query_map(params![connection_key, limit], |row| {
                            row.get::<_, String>(0)
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    mapped
                } else {
                    let mut statement = database.prepare(
                        "SELECT snapshot_json FROM chats ORDER BY updated_at_ms DESC LIMIT ?1",
                    )?;
                    let mapped = statement
                        .query_map([limit], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?;
                    mapped
                };
                Ok(rows)
            })
            .await?;
        rows.into_iter()
            .map(|json| serde_json::from_str(&json).map_err(StoreError::from))
            .collect()
    }

    async fn load_chat(&self, chat_id: &str) -> StoreResult<Option<ChatView>> {
        let chat_id = chat_id.to_string();
        let snapshot_json = self
            .connection
            .call(move |database| {
                database
                    .query_row(
                        "SELECT snapshot_json FROM chats WHERE id = ?1",
                        [chat_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
            })
            .await?;
        let Some(snapshot_json) = snapshot_json else {
            return Ok(None);
        };
        let chat = serde_json::from_str::<ChatSnapshot>(&snapshot_json)?;
        let messages = self.messages(&chat.id).await?;
        let events = self.events_after(&chat.id, 0).await?;
        Ok(Some(ChatView {
            chat,
            messages,
            events,
        }))
    }

    async fn save_snapshot_and_event(
        &self,
        snapshot: &ChatSnapshot,
        event: &ChatEvent,
    ) -> StoreResult<()> {
        persist_snapshot_event(&self.connection, snapshot, event, None).await
    }

    async fn save_snapshot_message_and_event(
        &self,
        snapshot: &ChatSnapshot,
        message: &ChatMessage,
        event: &ChatEvent,
    ) -> StoreResult<()> {
        persist_snapshot_event(&self.connection, snapshot, event, Some(message)).await
    }

    async fn save_approval(
        &self,
        chat_id: &str,
        message_sequence: u64,
        request: &ApprovalRequest,
    ) -> StoreResult<()> {
        let chat_id = chat_id.to_string();
        let approval_id = request.id.clone();
        let request_json = serde_json::to_string(request)?;
        let timestamp = now_ms();
        self.connection
            .call(move |database| {
                database.execute(
                    "INSERT INTO chat_turn_approvals(chat_id, message_sequence, approval_id, request_json, decision, created_at_ms, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?5)
                     ON CONFLICT(chat_id, message_sequence, approval_id) DO UPDATE SET
                       request_json = excluded.request_json, updated_at_ms = excluded.updated_at_ms
                     WHERE chat_turn_approvals.decision IS NULL",
                    params![chat_id, message_sequence, approval_id, request_json, timestamp],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn resolve_approval(
        &self,
        chat_id: &str,
        message_sequence: u64,
        approval_id: &str,
        decision: &str,
    ) -> StoreResult<()> {
        let chat_id = chat_id.to_string();
        let approval_id = approval_id.to_string();
        let decision = decision.to_string();
        let missing = format!("{chat_id}/{approval_id}");
        let updated = self
            .connection
            .call(move |database| {
                database.execute(
                    "UPDATE chat_turn_approvals SET decision = ?4, updated_at_ms = ?5
                     WHERE chat_id = ?1 AND message_sequence = ?2 AND approval_id = ?3 AND decision IS NULL",
                    params![chat_id, message_sequence, approval_id, decision, now_ms()],
                )
            })
            .await?;
        if updated == 0 {
            return Err(StoreError::ChatNotFound(missing));
        }
        Ok(())
    }

    async fn recover_interrupted_chats(&self) -> StoreResult<usize> {
        let interrupted = self
            .connection
            .call(|database| {
                let mut statement = database.prepare(
                    "SELECT snapshot_json FROM chats
                     WHERE status IN ('queued', 'running', 'approval_needed', 'cancelling')",
                )?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        let mut recovered = 0;
        for snapshot_json in interrupted {
            let mut snapshot = serde_json::from_str::<ChatSnapshot>(&snapshot_json)?;
            let events = self.events_after(&snapshot.id, 0).await?;
            snapshot.status = ChatStatus::Failed;
            snapshot.draft.clear();
            snapshot.error = Some(
                "The server restarted while a response was running. Your chat and provider session were preserved; send another message to resume."
                    .to_string(),
            );
            snapshot.updated_at_ms = now_ms();
            let event = ChatEvent {
                sequence: events.last().map_or(1, |event| event.sequence + 1),
                timestamp_ms: snapshot.updated_at_ms,
                kind: "chat_error".to_string(),
                payload: Value::Object(
                    [(
                        "message".to_string(),
                        Value::String(snapshot.error.clone().unwrap()),
                    )]
                    .into_iter()
                    .collect(),
                ),
            };
            self.save_snapshot_and_event(&snapshot, &event).await?;
            recovered += 1;
        }
        Ok(recovered)
    }
}

type QueuedMessageRow = (String, String, String, String, u64, String, u64, u64);

fn decode_queued_message(row: QueuedMessageRow) -> StoreResult<QueuedChatMessage> {
    let (id, chat_id, content, attachments, revision, metadata, created_at, updated_at) = row;
    Ok(QueuedChatMessage {
        id,
        chat_id,
        content,
        attachments: serde_json::from_str(&attachments)?,
        revision,
        created_at_unix_ms: created_at,
        updated_at_unix_ms: updated_at,
        metadata: serde_json::from_str(&metadata)?,
    })
}

fn read_queued_message_row(
    row: &tokio_rusqlite::rusqlite::Row<'_>,
) -> tokio_rusqlite::rusqlite::Result<QueuedMessageRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
    ))
}

#[async_trait]
impl DurableChatQueueStore for SqliteChatStore {
    type Error = StoreError;

    async fn enqueue(&self, message: &QueuedChatMessage) -> StoreResult<()> {
        let message = message.clone();
        let attachments = serde_json::to_string(&message.attachments)?;
        let metadata = serde_json::to_string(&message.metadata)?;
        self.connection
            .call(move |database| {
                database.execute(
                    "INSERT INTO chat_message_queue(id, chat_id, content, attachments_json, revision, metadata_json, created_at_ms, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![message.id, message.chat_id, message.content, attachments, message.revision, metadata, message.created_at_unix_ms, message.updated_at_unix_ms],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn list_queued(
        &self,
        chat_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> StoreResult<QueuedChatMessagePage> {
        let chat_id = chat_id.to_string();
        let offset = cursor
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let limit = u64::try_from(limit).unwrap_or(u64::MAX).min(200);
        let rows = self
            .connection
            .call(move |database| {
                let mut statement = database.prepare(
                    "SELECT id, chat_id, content, attachments_json, revision, metadata_json, created_at_ms, updated_at_ms
                     FROM chat_message_queue WHERE chat_id = ?1
                     ORDER BY created_at_ms ASC, id ASC LIMIT ?2 OFFSET ?3",
                )?;
                let rows = statement
                    .query_map(params![chat_id, limit, offset], read_queued_message_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        let row_count = rows.len();
        let messages = rows
            .into_iter()
            .map(decode_queued_message)
            .collect::<StoreResult<Vec<_>>>()?;
        let next_cursor = (row_count == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| (offset + limit).to_string());
        Ok(QueuedChatMessagePage {
            messages,
            next_cursor,
        })
    }

    async fn load_queued(
        &self,
        chat_id: &str,
        message_id: &str,
    ) -> StoreResult<Option<QueuedChatMessage>> {
        let chat_id = chat_id.to_string();
        let message_id = message_id.to_string();
        let row = self
            .connection
            .call(move |database| {
                database
                    .query_row(
                        "SELECT id, chat_id, content, attachments_json, revision, metadata_json, created_at_ms, updated_at_ms
                         FROM chat_message_queue WHERE chat_id = ?1 AND id = ?2",
                        params![chat_id, message_id],
                        read_queued_message_row,
                    )
                    .optional()
            })
            .await?;
        row.map(decode_queued_message).transpose()
    }

    async fn update_queued(
        &self,
        message: &QueuedChatMessage,
        expected_revision: u64,
    ) -> StoreResult<()> {
        let message = message.clone();
        let missing = message.id.clone();
        let attachments = serde_json::to_string(&message.attachments)?;
        let metadata = serde_json::to_string(&message.metadata)?;
        let updated = self
            .connection
            .call(move |database| {
                database.execute(
                    "UPDATE chat_message_queue
                     SET content = ?3, attachments_json = ?4, revision = ?5, metadata_json = ?6, updated_at_ms = ?7
                     WHERE id = ?1 AND chat_id = ?2 AND revision = ?8",
                    params![message.id, message.chat_id, message.content, attachments, message.revision, metadata, message.updated_at_unix_ms, expected_revision],
                )
            })
            .await?;
        if updated == 0 {
            return Err(StoreError::QueueConflict(missing));
        }
        Ok(())
    }

    async fn pop_queued(
        &self,
        chat_id: &str,
        message_id: &str,
    ) -> StoreResult<Option<QueuedChatMessage>> {
        let chat_id = chat_id.to_string();
        let message_id = message_id.to_string();
        let row = self
            .connection
            .call(move |database| {
                let transaction = database.transaction()?;
                let row = transaction
                    .query_row(
                        "SELECT id, chat_id, content, attachments_json, revision, metadata_json, created_at_ms, updated_at_ms
                         FROM chat_message_queue WHERE chat_id = ?1 AND id = ?2",
                        params![chat_id, message_id],
                        read_queued_message_row,
                    )
                    .optional()?;
                if row.is_some() {
                    transaction.execute(
                        "DELETE FROM chat_message_queue WHERE chat_id = ?1 AND id = ?2",
                        params![chat_id, message_id],
                    )?;
                }
                transaction.commit()?;
                Ok(row)
            })
            .await?;
        row.map(decode_queued_message).transpose()
    }

    async fn pop_next_queued(&self, chat_id: &str) -> StoreResult<Option<QueuedChatMessage>> {
        let chat_id = chat_id.to_string();
        let row = self
            .connection
            .call(move |database| {
                let transaction = database.transaction()?;
                let row = transaction
                    .query_row(
                        "SELECT id, chat_id, content, attachments_json, revision, metadata_json, created_at_ms, updated_at_ms
                         FROM chat_message_queue WHERE chat_id = ?1
                         ORDER BY created_at_ms ASC, id ASC LIMIT 1",
                        [&chat_id],
                        read_queued_message_row,
                    )
                    .optional()?;
                if let Some((id, ..)) = row.as_ref() {
                    transaction.execute(
                        "DELETE FROM chat_message_queue WHERE chat_id = ?1 AND id = ?2",
                        params![chat_id, id],
                    )?;
                }
                transaction.commit()?;
                Ok(row)
            })
            .await?;
        row.map(decode_queued_message).transpose()
    }
}

async fn persist_snapshot_event(
    connection: &tokio_rusqlite::Connection,
    snapshot: &ChatSnapshot,
    event: &ChatEvent,
    message: Option<&ChatMessage>,
) -> StoreResult<()> {
    let snapshot_json = serde_json::to_string(snapshot)?;
    let payload_json = serde_json::to_string(&event.payload)?;
    let snapshot = snapshot.clone();
    let event = event.clone();
    let message = message.cloned();
    let message_attachments_json = message
        .as_ref()
        .map(|message| serde_json::to_string(&message.attachments))
        .transpose()?;
    connection
        .call(move |database| {
            let transaction = database.transaction()?;
            let updated = transaction.execute(
                "UPDATE chats
                 SET snapshot_json = ?2, connection_key = ?3, status = ?4, updated_at_ms = ?5
                 WHERE id = ?1",
                params![snapshot.id, snapshot_json, snapshot.connection_key, status_name(snapshot.status), snapshot.updated_at_ms],
            )?;
            if updated == 0 {
                return Err(tokio_rusqlite::rusqlite::Error::QueryReturnedNoRows);
            }
            if let Some(message) = message {
                let attachments_json = message_attachments_json.unwrap_or_else(|| "[]".to_string());
                transaction.execute(
                    "INSERT INTO chat_messages(chat_id, sequence, role, content, attachments_json, created_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![snapshot.id, message.sequence, message.role, message.content, attachments_json, message.created_at_ms],
                )?;
                transaction.execute(
                    "DELETE FROM chat_messages WHERE chat_id = ?1 AND sequence <= ?2",
                    params![snapshot.id, message.sequence.saturating_sub(MAX_MESSAGES_PER_CHAT)],
                )?;
            }
            transaction.execute(
                "INSERT INTO chat_events(chat_id, sequence, timestamp_ms, kind, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![snapshot.id, event.sequence, event.timestamp_ms, event.kind, payload_json],
            )?;
            transaction.execute(
                "DELETE FROM chat_events WHERE chat_id = ?1 AND sequence <= ?2",
                params![snapshot.id, event.sequence.saturating_sub(MAX_EVENTS_PER_CHAT)],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await?;
    Ok(())
}

const fn status_name(status: ChatStatus) -> &'static str {
    match status {
        ChatStatus::Queued => "queued",
        ChatStatus::Running => "running",
        ChatStatus::ApprovalNeeded => "approval_needed",
        ChatStatus::InputNeeded => "input_needed",
        ChatStatus::Cancelling => "cancelling",
        ChatStatus::Succeeded => "succeeded",
        ChatStatus::Failed => "failed",
        ChatStatus::Cancelled => "cancelled",
    }
}

const fn ssh_authentication_name(authentication: StoredSshAuthentication) -> &'static str {
    match authentication {
        StoredSshAuthentication::Agent => "agent",
        StoredSshAuthentication::IdentityFile => "identity_file",
        StoredSshAuthentication::Password => "password",
    }
}

fn parse_ssh_authentication(value: &str) -> StoreResult<StoredSshAuthentication> {
    match value {
        "agent" => Ok(StoredSshAuthentication::Agent),
        "identity_file" => Ok(StoredSshAuthentication::IdentityFile),
        "password" => Ok(StoredSshAuthentication::Password),
        value => Err(StoreError::Crypto(format!(
            "stored SSH authentication kind `{value}` is invalid"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionKind, DemoScenario, ExecutionMode, PermissionChoice, ProviderChoice};
    use std::collections::BTreeMap;

    fn snapshot(id: &str, status: ChatStatus) -> ChatSnapshot {
        ChatSnapshot {
            id: id.to_string(),
            title: "Persist this conversation".to_string(),
            mode: ExecutionMode::Demo,
            provider: ProviderChoice::Claude,
            permission: PermissionChoice::Plan,
            scenario: DemoScenario::Approval,
            target: "Deterministic subprocess".to_string(),
            connection_kind: ConnectionKind::Local,
            connection_id: None,
            connection_key: "local".to_string(),
            working_directory: ".".to_string(),
            status,
            draft: String::new(),
            session_id: None,
            model: None,
            reasoning: None,
            usage: temps_agent_runtime::Usage::default(),
            account_usage: None,
            harness_options: BTreeMap::new(),
            error: None,
            created_at_ms: 10,
            updated_at_ms: 10,
        }
    }

    fn user_message() -> ChatMessage {
        ChatMessage {
            sequence: 1,
            role: "user".to_string(),
            content: "Persist this conversation.".to_string(),
            attachments: Vec::new(),
            created_at_ms: 10,
        }
    }

    #[tokio::test]
    async fn chat_messages_and_events_survive_reopening_the_database() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("runtime.sqlite3");
        let store = SqliteChatStore::open(&path).await.expect("open store");
        let mut chat = snapshot("chat_0001", ChatStatus::Running);
        chat.working_directory = "/remote/projects/runtime".to_string();
        store
            .create_chat(&chat, &user_message())
            .await
            .expect("create chat");
        chat.status = ChatStatus::Succeeded;
        chat.updated_at_ms = 20;
        store
            .save_snapshot_message_and_event(
                &chat,
                &ChatMessage {
                    sequence: 2,
                    role: "assistant".to_string(),
                    content: "Stored answer".to_string(),
                    attachments: Vec::new(),
                    created_at_ms: 20,
                },
                &ChatEvent {
                    sequence: 1,
                    timestamp_ms: 20,
                    kind: "message_appended".to_string(),
                    payload: serde_json::json!({ "sequence": 2, "role": "assistant", "content": "Stored answer", "created_at_ms": 20 }),
                },
            )
            .await
            .expect("save message");
        drop(store);

        let reopened = SqliteChatStore::open(&path).await.expect("reopen store");
        let view = reopened
            .load_chat("chat_0001")
            .await
            .expect("load chat")
            .expect("stored chat");
        assert_eq!(view.chat.status, ChatStatus::Succeeded);
        assert_eq!(
            view.chat.working_directory, "/remote/projects/runtime",
            "the target-relative working directory must survive a restart"
        );
        assert_eq!(view.messages.len(), 2);
        assert_eq!(view.messages[1].content, "Stored answer");
        assert_eq!(view.events.len(), 1);
    }

    #[tokio::test]
    async fn chat_index_is_scoped_to_the_execution_target() {
        let store = SqliteChatStore::open(Path::new(":memory:"))
            .await
            .expect("open store");
        let local = snapshot("chat_local", ChatStatus::Succeeded);
        let mut remote = snapshot("chat_remote", ChatStatus::Succeeded);
        remote.connection_kind = ConnectionKind::Ssh;
        remote.connection_id = Some("studio".to_string());
        remote.connection_key = "ssh:saved:studio".to_string();
        remote.target = "studio@example.test".to_string();

        store
            .create_chat(&local, &user_message())
            .await
            .expect("create local chat");
        store
            .create_chat(&remote, &user_message())
            .await
            .expect("create remote chat");

        let local_chats = store
            .list_chats(Some("local"), 20)
            .await
            .expect("list local chats");
        let remote_chats = store
            .list_chats(Some("ssh:saved:studio"), 20)
            .await
            .expect("list remote chats");

        assert_eq!(
            local_chats
                .iter()
                .map(|chat| chat.id.as_str())
                .collect::<Vec<_>>(),
            ["chat_local"]
        );
        assert_eq!(
            remote_chats
                .iter()
                .map(|chat| chat.id.as_str())
                .collect::<Vec<_>>(),
            ["chat_remote"]
        );
    }

    #[tokio::test]
    async fn interrupted_chat_preserves_messages_and_becomes_a_typed_failure() {
        let store = SqliteChatStore::open(Path::new(":memory:"))
            .await
            .expect("open store");
        store
            .create_chat(
                &snapshot("chat_0001", ChatStatus::ApprovalNeeded),
                &user_message(),
            )
            .await
            .expect("create chat");

        assert_eq!(store.recover_interrupted_chats().await.expect("recover"), 1);
        let view = store
            .load_chat("chat_0001")
            .await
            .expect("load chat")
            .expect("stored chat");
        assert_eq!(view.chat.status, ChatStatus::Failed);
        assert_eq!(view.messages.len(), 1);
        assert!(view
            .chat
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("preserved"));
        assert_eq!(view.events[0].kind, "chat_error");
    }

    #[tokio::test]
    async fn ssh_passwords_are_encrypted_and_survive_reopening_the_database() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("runtime.sqlite3");
        let store = SqliteChatStore::open(&path).await.expect("open store");
        let connection = StoredSshConnection {
            id: "ssh_0001".to_string(),
            label: "Studio".to_string(),
            host: "192.168.1.9".to_string(),
            user: Some("joseviejo".to_string()),
            port: Some(22),
            authentication: StoredSshAuthentication::Password,
            identity_file: None,
            password: Some("not-plaintext".to_string()),
            known_hosts_file: None,
            accept_new_host_key: false,
            created_at_ms: 10,
            updated_at_ms: 10,
        };
        store
            .save_ssh_connection(&connection)
            .await
            .expect("save connection");
        let encrypted = store
            .connection
            .call(|database| {
                database.query_row(
                    "SELECT password_encrypted FROM ssh_connections WHERE id = 'ssh_0001'",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
            })
            .await
            .expect("read encrypted password");
        assert!(!String::from_utf8_lossy(&encrypted).contains("not-plaintext"));
        drop(store);

        let reopened = SqliteChatStore::open(&path).await.expect("reopen store");
        let loaded = reopened
            .load_ssh_connection("ssh_0001")
            .await
            .expect("load connection")
            .expect("saved connection");
        assert_eq!(loaded.password.as_deref(), Some("not-plaintext"));
        let summaries = reopened
            .list_ssh_connections()
            .await
            .expect("list connections");
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].has_password);
    }
}
