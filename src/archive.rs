//! Archive tier (`--archive FILTER`): every message published on a matching topic is appended to
//! an Apache Iceberg table (Parquet files on a local or S3 warehouse, in a SQL or REST catalog),
//! for analytics and replay with Spark, Trino, DuckDB, Athena...
//!
//! The cores only batch rows and hand them over with a non-blocking send; one archiver thread
//! (outside the cores, with a single-threaded tokio runtime) writes a Parquet file per partition
//! and commits a table snapshot every `commit_interval`. Rows are sorted by topic then time, and
//! partitioned by day.
//!
//! Best effort: rows not committed yet are lost on a crash, and dropped (and counted) when the
//! archiver falls behind by `MAX_PENDING_BATCHES` batches or `MAX_BUFFERED_ROWS` rows. The
//! Iceberg writer is behind the `iceberg` cargo feature; without it `Archiver::start` fails.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::codec;

#[derive(Clone, Debug)]
pub struct ArchiveConfig {
    /// Topic filters of the archived messages.
    pub filters: Vec<Box<str>>,
    /// Catalog: `sqlite:PATH` (SQL catalog in a SQLite file) or `http(s)://...` (REST catalog).
    pub catalog: String,
    /// Where the table files go: `file:///DIR` or `s3://BUCKET/PATH`.
    pub warehouse: String,
    /// `NAMESPACE.TABLE`, created if missing.
    pub table: String,
    pub commit_interval: Duration,
    /// More catalog and storage properties (`s3.endpoint`, `s3.region`, `token`...).
    pub props: Vec<(String, String)>,
    /// CPUs the archiver thread stays off: the broker's cores.
    pub avoid_cores: Vec<usize>,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        ArchiveConfig {
            filters: Vec::new(),
            catalog: String::new(),
            warehouse: String::new(),
            table: "mqtt.messages".into(),
            commit_interval: Duration::from_secs(10),
            props: Vec::new(),
            avoid_cores: Vec::new(),
        }
    }
}

/// One archived message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// Reception time, UNIX microseconds.
    pub ts_us: i64,
    pub topic: Box<str>,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    /// The publisher (empty for a will published by the broker).
    pub client_id: Box<str>,
}

/// Rows in columns, as the Arrow arrays of the table will hold them: appending a row allocates
/// nothing (past the growth of the buffers), and the archiver turns them into arrays without
/// copying.
#[derive(Debug)]
pub struct Rows {
    ts_us: Vec<i64>,
    /// Offsets into `topics` (n + 1 of them), and so on.
    topic_offsets: Vec<i32>,
    topics: Vec<u8>,
    payload_offsets: Vec<i64>,
    payloads: Vec<u8>,
    qos: Vec<i32>,
    retain: Vec<bool>,
    client_offsets: Vec<i32>,
    clients: Vec<u8>,
}

impl Default for Rows {
    fn default() -> Self {
        Rows {
            ts_us: Vec::new(),
            topic_offsets: vec![0],
            topics: Vec::new(),
            payload_offsets: vec![0],
            payloads: Vec::new(),
            qos: Vec::new(),
            retain: Vec::new(),
            client_offsets: vec![0],
            clients: Vec::new(),
        }
    }
}

impl Rows {
    pub fn push(&mut self, ts_us: i64, topic: &str, payload: &[u8], qos: u8, retain: bool, client_id: &str) {
        self.ts_us.push(ts_us);
        self.topics.extend_from_slice(topic.as_bytes());
        self.topic_offsets.push(self.topics.len() as i32);
        self.payloads.extend_from_slice(payload);
        self.payload_offsets.push(self.payloads.len() as i64);
        self.qos.push(qos as i32);
        self.retain.push(retain);
        self.clients.extend_from_slice(client_id.as_bytes());
        self.client_offsets.push(self.clients.len() as i32);
    }

    pub fn len(&self) -> usize {
        self.ts_us.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts_us.is_empty()
    }

    #[cfg_attr(not(feature = "iceberg"), allow(dead_code))]
    fn append(&mut self, other: &Rows) {
        fn offsets<T: Copy + std::ops::Add<Output = T>>(to: &mut Vec<T>, from: &[T]) {
            let base = *to.last().unwrap();
            to.extend(from[1..].iter().map(|&o| base + o));
        }
        self.ts_us.extend_from_slice(&other.ts_us);
        offsets(&mut self.topic_offsets, &other.topic_offsets);
        self.topics.extend_from_slice(&other.topics);
        offsets(&mut self.payload_offsets, &other.payload_offsets);
        self.payloads.extend_from_slice(&other.payloads);
        self.qos.extend_from_slice(&other.qos);
        self.retain.extend_from_slice(&other.retain);
        offsets(&mut self.client_offsets, &other.client_offsets);
        self.clients.extend_from_slice(&other.clients);
    }

    fn topic(&self, i: usize) -> &[u8] {
        &self.topics[self.topic_offsets[i] as usize..self.topic_offsets[i + 1] as usize]
    }

    /// Row `i`, decoded (for tests and tools).
    pub fn row(&self, i: usize) -> Row {
        let range = |o: &[i32], i: usize| o[i] as usize..o[i + 1] as usize;
        Row {
            ts_us: self.ts_us[i],
            topic: String::from_utf8_lossy(self.topic(i)).into(),
            payload: self.payloads[self.payload_offsets[i] as usize..self.payload_offsets[i + 1] as usize].to_vec(),
            qos: self.qos[i] as u8,
            retain: self.retain[i],
            client_id: String::from_utf8_lossy(&self.clients[range(&self.client_offsets, i)]).into(),
        }
    }

    /// The order the archiver writes rows in: by topic, then time (long runs of one topic
    /// compress well, and readers skip files and pages by topic range).
    #[cfg_attr(not(feature = "iceberg"), allow(dead_code))]
    fn order(&self) -> Vec<u32> {
        let mut order: Vec<u32> = (0..self.len() as u32).collect();
        order.sort_by(|&a, &b| {
            let (a, b) = (a as usize, b as usize);
            self.topic(a).cmp(self.topic(b)).then(self.ts_us[a].cmp(&self.ts_us[b]))
        });
        order
    }
}

/// Rows a core sends at once, at most (it sends what it has at least every second).
pub const BATCH_ROWS: usize = 4096;
const MAX_PENDING_BATCHES: usize = 1024;
/// The archiver commits when it has this many rows, without waiting for the commit interval.
#[cfg_attr(not(feature = "iceberg"), allow(dead_code))]
const COMMIT_ROWS: usize = 1 << 20;
/// Rows kept while commits fail; past that, new rows are dropped.
#[cfg_attr(not(feature = "iceberg"), allow(dead_code))]
const MAX_BUFFERED_ROWS: usize = 4 << 20;

#[cfg_attr(not(feature = "iceberg"), allow(dead_code))]
enum Command {
    Rows(Box<Rows>),
    Stop,
}

#[derive(Default, Debug)]
pub struct Stats {
    /// Rows in committed snapshots.
    pub committed: AtomicU64,
    /// Rows dropped (archiver behind, or commits failing).
    pub dropped: AtomicU64,
    /// Snapshots committed.
    pub commits: AtomicU64,
}

/// What a core holds: the filters, and the way to the archiver.
#[derive(Clone)]
pub struct ArchiveSender {
    filters: Arc<[Box<str>]>,
    tx: SyncSender<Command>,
    stats: Arc<Stats>,
}

impl ArchiveSender {
    pub fn matches(&self, topic: &str) -> bool {
        self.filters.iter().any(|f| codec::matches(f, topic))
    }

    /// Never blocks: if the archiver is too far behind, the rows are dropped.
    pub fn send(&self, rows: Rows) {
        let n = rows.len() as u64;
        match self.tx.try_send(Command::Rows(Box::new(rows))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.stats.dropped.fetch_add(n, Ordering::Relaxed);
            }
        }
    }
}

pub struct Archiver {
    sender: ArchiveSender,
    thread: Option<JoinHandle<()>>,
}

impl Archiver {
    /// Connects to the catalog and creates the table if needed (errors come back here), then
    /// runs the archiver thread.
    pub fn start(config: &ArchiveConfig) -> io::Result<Archiver> {
        let (tx, rx) = mpsc::sync_channel(MAX_PENDING_BATCHES);
        let stats = Arc::new(Stats::default());
        let filters: Arc<[Box<str>]> = config.filters.clone().into();
        let thread = imp::spawn(config.clone(), rx, stats.clone())?;
        Ok(Archiver { sender: ArchiveSender { filters, tx, stats }, thread: Some(thread) })
    }

    pub fn sender(&self) -> ArchiveSender {
        self.sender.clone()
    }

    pub fn stats(&self) -> &Stats {
        &self.sender.stats
    }

    /// Commits what was received and ends the thread (after `broker::stop`, so that the cores
    /// have sent their last rows).
    pub fn stop(mut self) {
        let _ = self.sender.tx.send(Command::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Every row of the table, in file order (for tests and tools; it loads the whole table).
pub fn read(config: &ArchiveConfig) -> io::Result<Vec<Row>> {
    imp::read(config)
}

#[cfg(not(feature = "iceberg"))]
mod imp {
    use super::*;

    pub fn spawn(_: ArchiveConfig, _: mpsc::Receiver<Command>, _: Arc<Stats>) -> io::Result<JoinHandle<()>> {
        Err(unsupported())
    }

    pub fn read(_: &ArchiveConfig) -> io::Result<Vec<Row>> {
        Err(unsupported())
    }

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "--archive needs croisette built with --features iceberg")
    }
}

#[cfg(feature = "iceberg")]
mod imp {
    use std::collections::HashMap;
    use std::time::Instant;

    use arrow_array::{
        Array, ArrayRef, BooleanArray, Int32Array, LargeBinaryArray, RecordBatch, StringArray,
        TimestampMicrosecondArray, UInt32Array,
    };
    use arrow_buffer::{Buffer, OffsetBuffer};
    use arrow_schema::Field;
    use arrow_select::take::take_record_batch;
    use iceberg::arrow::{schema_to_arrow_schema, RecordBatchPartitionSplitter};
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
    use iceberg::writer::partitioning::PartitioningWriter;
    use iceberg::{Catalog, CatalogBuilder, TableCreation, TableIdent};
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    use super::*;

    fn err(e: impl std::fmt::Display) -> io::Error {
        io::Error::other(format!("archive: {e}"))
    }

    pub fn spawn(config: ArchiveConfig, rx: mpsc::Receiver<Command>, stats: Arc<Stats>) -> io::Result<JoinHandle<()>> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let (catalog, ident) = runtime.block_on(open(&config)).map_err(err)?;
        let thread = std::thread::Builder::new().name("croisette-archive".into()).spawn(move || {
            if let Err(e) = crate::net::avoid_cores(&config.avoid_cores) {
                eprintln!("croisette: archive thread affinity: {e}");
            }
            let mut a = Archive { catalog, ident, config, stats, rows: Rows::default(), pending: Vec::new(), files: 0 };
            let mut next_commit = Instant::now() + a.config.commit_interval;
            loop {
                let wait = next_commit.saturating_duration_since(Instant::now());
                let stop = match rx.recv_timeout(wait) {
                    Ok(Command::Rows(rows)) => {
                        a.take(&rows);
                        false
                    }
                    Ok(Command::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => true,
                    Err(mpsc::RecvTimeoutError::Timeout) => false,
                };
                if stop || Instant::now() >= next_commit || a.rows.len() >= COMMIT_ROWS {
                    runtime.block_on(a.commit());
                    next_commit = Instant::now() + a.config.commit_interval;
                }
                if stop {
                    return;
                }
            }
        })?;
        Ok(thread)
    }

    pub fn read(config: &ArchiveConfig) -> io::Result<Vec<Row>> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Int32Type, TimestampMicrosecondType};
        use futures::TryStreamExt;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime
            .block_on(async {
                let (catalog, ident) = open(config).await?;
                let table = catalog.load_table(&ident).await?;
                let batches: Vec<RecordBatch> =
                    table.scan().select_all().build()?.to_arrow().await?.try_collect().await?;
                let mut rows = Vec::new();
                for b in batches {
                    let (ts, topic) =
                        (b.column(0).as_primitive::<TimestampMicrosecondType>(), b.column(1).as_string::<i32>());
                    let (payload, qos) = (b.column(2).as_binary::<i64>(), b.column(3).as_primitive::<Int32Type>());
                    let (retain, client_id) = (b.column(4).as_boolean(), b.column(5).as_string::<i32>());
                    for i in 0..b.num_rows() {
                        rows.push(Row {
                            ts_us: ts.value(i),
                            topic: topic.value(i).into(),
                            payload: payload.value(i).to_vec(),
                            qos: qos.value(i) as u8,
                            retain: retain.value(i),
                            client_id: client_id.value(i).into(),
                        });
                    }
                }
                Ok::<_, iceberg::Error>(rows)
            })
            .map_err(err)
    }

    async fn open(config: &ArchiveConfig) -> iceberg::Result<(Box<dyn Catalog>, TableIdent)> {
        let mut props: HashMap<String, String> = config.props.iter().cloned().collect();
        props.insert("uri".into(), config.catalog.clone());
        props.insert("warehouse".into(), config.warehouse.clone());
        let storage = Arc::new(OpenDalResolvingStorageFactory::new());
        let catalog: Box<dyn Catalog> = if config.catalog.starts_with("sqlite:") {
            if !config.catalog.contains('?') {
                props.insert("uri".into(), format!("{}?mode=rwc", config.catalog));
                // create the file
            }
            props.insert("sql_bind_style".into(), "QMark".into());
            let builder = iceberg_catalog_sql::SqlCatalogBuilder::default().with_storage_factory(storage);
            Box::new(builder.load("croisette", props).await?)
        } else {
            let builder = iceberg_catalog_rest::RestCatalogBuilder::default().with_storage_factory(storage);
            Box::new(builder.load("croisette", props).await?)
        };
        let ident = TableIdent::from_strs(config.table.split('.'))?;
        let ns = ident.namespace().clone();
        if !catalog.namespace_exists(&ns).await? {
            catalog.create_namespace(&ns, HashMap::new()).await?;
        }
        if !catalog.table_exists(&ident).await? {
            catalog.create_table(&ns, creation(ident.name())?).await?;
        }
        Ok((catalog, ident))
    }

    fn creation(name: &str) -> iceberg::Result<TableCreation> {
        let field = |id, name, t| Arc::new(NestedField::required(id, name, Type::Primitive(t)));
        let schema = Schema::builder()
            .with_fields(vec![
                field(1, "ts", PrimitiveType::Timestamptz),
                field(2, "topic", PrimitiveType::String),
                field(3, "payload", PrimitiveType::Binary),
                field(4, "qos", PrimitiveType::Int),
                field(5, "retain", PrimitiveType::Boolean),
                field(6, "client_id", PrimitiveType::String),
            ])
            .build()?;
        let spec = UnboundPartitionSpec::builder().add_partition_field(1, "ts_day", Transform::Day)?.build();
        Ok(TableCreation::builder().name(name.into()).schema(schema).partition_spec(spec).build())
    }

    struct Archive {
        catalog: Box<dyn Catalog>,
        ident: TableIdent,
        config: ArchiveConfig,
        stats: Arc<Stats>,
        rows: Rows,
        /// Sorted batches of rows whose commit failed, for the next attempt.
        pending: Vec<RecordBatch>,
        /// Files written by this process (for unique names).
        files: u64,
    }

    impl Archive {
        fn take(&mut self, rows: &Rows) {
            let pending: usize = self.pending.iter().map(|b| b.num_rows()).sum();
            if pending + self.rows.len() + rows.len() > MAX_BUFFERED_ROWS {
                self.stats.dropped.fetch_add(rows.len() as u64, Ordering::Relaxed);
                return;
            }
            self.rows.append(rows);
        }

        /// Writes the buffered rows and commits them in one snapshot; on failure they stay
        /// buffered for the next attempt.
        async fn commit(&mut self) {
            if !self.rows.is_empty() {
                match record_batch(std::mem::take(&mut self.rows)) {
                    Ok(b) => self.pending.push(b),
                    Err(e) => eprintln!("croisette: archive rows dropped: {e}"),
                }
            }
            if self.pending.is_empty() {
                return;
            }
            match self.write().await {
                Ok(()) => {
                    let n: usize = self.pending.drain(..).map(|b| b.num_rows()).sum();
                    self.stats.committed.fetch_add(n as u64, Ordering::Relaxed);
                    self.stats.commits.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => eprintln!("croisette: archive commit failed, will retry: {e}"),
            }
        }

        async fn write(&mut self) -> iceberg::Result<()> {
            let table = self.catalog.load_table(&self.ident).await?;
            let schema = table.metadata().current_schema().clone();
            let arrow = Arc::new(schema_to_arrow_schema(&schema)?);
            let spec = table.metadata().default_partition_spec().clone();
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(schema.clone(), spec)?;
            let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())).build();
            // Unique across restarts and processes: start time, pid, sequence.
            self.files += 1;
            let prefix = format!("{}-{}-{}", process_start_us(), std::process::id(), self.files);
            let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(props, schema),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet),
            );
            let mut writer = FanoutWriter::new(DataFileWriterBuilder::new(rolling));
            for batch in &self.pending {
                // The table's Arrow schema carries the field ids the writer needs.
                let batch = batch.clone().with_schema(arrow.clone()).map_err(arrow_err)?;
                for (key, part) in splitter.split(&batch)? {
                    writer.write(key, part).await?;
                }
            }
            let files = writer.close().await?;
            let tx = Transaction::new(&table);
            let tx = tx.fast_append().add_data_files(files).apply(tx)?;
            tx.commit(self.catalog.as_ref()).await?;
            Ok(())
        }
    }

    fn process_start_us() -> u128 {
        static START: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
        *START.get_or_init(|| {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros()
        })
    }

    fn arrow_err(e: arrow_schema::ArrowError) -> iceberg::Error {
        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
    }

    /// The rows as Arrow arrays (their buffers moved, not copied), in `Rows::order`.
    fn record_batch(rows: Rows) -> iceberg::Result<RecordBatch> {
        let order = UInt32Array::from(rows.order());
        let strings = |offsets: Vec<i32>, bytes: Vec<u8>| {
            StringArray::try_new(OffsetBuffer::new(offsets.into()), Buffer::from_vec(bytes), None)
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::new(rows.ts_us.into(), None).with_timezone("+00:00")),
            Arc::new(strings(rows.topic_offsets, rows.topics).map_err(arrow_err)?),
            Arc::new(LargeBinaryArray::new(
                OffsetBuffer::new(rows.payload_offsets.into()),
                Buffer::from_vec(rows.payloads),
                None,
            )),
            Arc::new(Int32Array::new(rows.qos.into(), None)),
            Arc::new(BooleanArray::from(rows.retain)),
            Arc::new(strings(rows.client_offsets, rows.clients).map_err(arrow_err)?),
        ];
        let names = ["ts", "topic", "payload", "qos", "retain", "client_id"];
        let fields: Vec<Field> =
            names.iter().zip(&columns).map(|(n, c)| Field::new(*n, c.data_type().clone(), false)).collect();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).map_err(arrow_err)?;
        take_record_batch(&batch, &order).map_err(arrow_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_append_and_order() {
        let mut a = Rows::default();
        a.push(2, "b", b"x", 1, false, "c1");
        a.push(5, "a", b"", 0, true, "");
        let mut b = Rows::default();
        b.push(1, "a", b"yz", 2, false, "c2");
        a.append(&b);
        assert_eq!(a.len(), 3);
        let row = |ts, t: &str, p: &[u8], qos, retain, c: &str| Row {
            ts_us: ts,
            topic: t.into(),
            payload: p.to_vec(),
            qos,
            retain,
            client_id: c.into(),
        };
        assert_eq!(a.row(2), row(1, "a", b"yz", 2, false, "c2"));
        let ordered: Vec<Row> = a.order().into_iter().map(|i| a.row(i as usize)).collect();
        assert_eq!(
            ordered,
            vec![row(1, "a", b"yz", 2, false, "c2"), row(5, "a", b"", 0, true, ""), row(2, "b", b"x", 1, false, "c1")]
        );
    }
}
