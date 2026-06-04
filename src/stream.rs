use anyhow::{Result, anyhow};
use arrow::array::{ArrayBuilder, Int64Builder};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::config::FletchConfig;
use crate::sink::BackgroundSink;
use crate::types::{ChannelBuilder, FletchType, FletchValue};
use crate::workspace::FletchWorkspace;

const DEFAULT_BATCH_CAPACITY: usize = 100_000;

pub trait FletchSchema {
    fn stream_name() -> &'static str;
    fn configure_builder(builder: FletchStreamBuilder) -> Result<FletchStreamBuilder>;
}

pub struct Stream<T> {
    inner: FletchStream,
    _marker: PhantomData<T>,
}

impl<T: FletchSchema> Stream<T> {
    pub async fn try_new(workspace: &FletchWorkspace, run_id: &str) -> Result<Self> {
        let builder = FletchStreamBuilder::new(workspace, T::stream_name());
        let builder = T::configure_builder(builder)?;
        builder.build(run_id).await
    }
}

impl<T> Stream<T> {
    pub fn write<V: FletchType>(
        &mut self,
        timestamp_ns: i64,
        channel: &str,
        value: V,
    ) -> Result<()> {
        self.inner.write(timestamp_ns, channel, value)
    }

    pub fn close(self) -> Result<()> {
        self.inner.close()
    }

    pub fn into_inner(self) -> FletchStream {
        self.inner
    }
}

#[derive(Clone)]
struct ChannelSpec {
    name: String,
    data_type: arrow::datatypes::DataType,
    builder_factory: fn(usize) -> ChannelBuilder,
}

pub struct FletchStreamBuilder {
    workspace: FletchWorkspace,
    stream_name: String,
    channels: Vec<ChannelSpec>,
    metadata: BTreeMap<String, String>,
    batch_capacity: usize,
}

impl FletchStreamBuilder {
    pub fn new(workspace: &FletchWorkspace, stream_name: impl Into<String>) -> Self {
        Self {
            workspace: workspace.clone(),
            stream_name: stream_name.into(),
            channels: Vec::new(),
            metadata: BTreeMap::new(),
            batch_capacity: DEFAULT_BATCH_CAPACITY,
        }
    }

    pub fn channel<T: FletchType>(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name == "timestamp_ns" {
            return Err(anyhow!("channel name `timestamp_ns` is reserved"));
        }
        if self.channels.iter().any(|channel| channel.name == name) {
            return Err(anyhow!("duplicate channel `{}`", name));
        }
        self.channels.push(ChannelSpec {
            name,
            data_type: T::data_type(),
            builder_factory: T::new_builder,
        });
        Ok(self)
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn metadata_pairs<I, K, V>(mut self, metadata: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in metadata {
            self.metadata.insert(key.into(), value.into());
        }
        self
    }

    pub fn batch_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(anyhow!("batch capacity must be greater than zero"));
        }
        self.batch_capacity = capacity;
        Ok(self)
    }

    pub async fn build<T>(self, run_id: &str) -> Result<Stream<T>> {
        let inner = self.build_dynamic(run_id).await?;
        Ok(Stream {
            inner,
            _marker: PhantomData,
        })
    }

    pub async fn build_dynamic(self, run_id: &str) -> Result<FletchStream> {
        if self.channels.is_empty() {
            return Err(anyhow!("at least one channel is required"));
        }

        let mut fields = vec![Field::new(
            "timestamp_ns",
            arrow::datatypes::DataType::Int64,
            false,
        )];
        fields.extend(
            self.channels
                .iter()
                .map(|channel| Field::new(channel.name.clone(), channel.data_type.clone(), true)),
        );
        let schema = Arc::new(Schema::new(fields));
        let config = FletchConfig::init(&self.workspace, &self.stream_name, run_id, self.metadata)?;
        let sink = BackgroundSink::spawn(config, schema.clone())?;
        let channel_order = self
            .channels
            .iter()
            .map(|channel| channel.name.clone())
            .collect::<Vec<_>>();
        let channels = self
            .channels
            .into_iter()
            .map(|spec| {
                let name = spec.name;
                let channel = Channel {
                    builder: (spec.builder_factory)(self.batch_capacity),
                    pending: None,
                    data_type: spec.data_type,
                };
                (name, channel)
            })
            .collect::<HashMap<_, _>>();

        Ok(FletchStream {
            sink,
            schema,
            timestamps: Int64Builder::with_capacity(self.batch_capacity),
            current_ts: None,
            channel_order,
            channels,
            batch_capacity: self.batch_capacity,
        })
    }
}

struct Channel {
    builder: ChannelBuilder,
    pending: Option<FletchValue>,
    data_type: arrow::datatypes::DataType,
}

pub struct FletchStream {
    sink: BackgroundSink,
    schema: Arc<Schema>,
    timestamps: Int64Builder,
    current_ts: Option<i64>,
    channel_order: Vec<String>,
    channels: HashMap<String, Channel>,
    batch_capacity: usize,
}

impl FletchStream {
    pub fn write<T: FletchType>(
        &mut self,
        timestamp_ns: i64,
        channel: &str,
        value: T,
    ) -> Result<()> {
        let registered = self
            .channels
            .get(channel)
            .ok_or_else(|| anyhow!("unknown channel `{}`", channel))?;
        if registered.data_type != T::data_type() {
            return Err(anyhow!(
                "channel `{}` expects {:?}, got {:?}",
                channel,
                registered.data_type,
                T::data_type()
            ));
        }

        if let Some(current) = self.current_ts {
            if current != timestamp_ns {
                self.commit_pending_row()?;
                self.current_ts = Some(timestamp_ns);
            }
        } else {
            self.current_ts = Some(timestamp_ns);
        }

        self.channels
            .get_mut(channel)
            .expect("channel was checked above")
            .pending = Some(value.into_value());

        if self.timestamps.len() >= self.batch_capacity {
            self.flush_batch()?;
        }

        Ok(())
    }

    fn commit_pending_row(&mut self) -> Result<()> {
        if let Some(ts) = self.current_ts {
            self.timestamps.append_value(ts);
            for channel_name in &self.channel_order {
                let channel = self
                    .channels
                    .get_mut(channel_name)
                    .expect("channel order should match channel map");
                channel.builder.append(channel.pending.take())?;
            }
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        self.commit_pending_row()?;
        self.current_ts = None;

        if self.timestamps.is_empty() {
            return Ok(());
        }

        let ts_array = Arc::new(self.timestamps.finish()) as arrow::array::ArrayRef;
        let mut columns = vec![ts_array.clone()];
        for channel_name in &self.channel_order {
            let channel = self
                .channels
                .get_mut(channel_name)
                .expect("channel order should match channel map");
            columns.push(channel.builder.finish());
        }

        let raw_batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_column = SortColumn {
            values: ts_array,
            options: Some(sort_options),
        };
        let sorted_indices = lexsort_to_indices(&[sort_column], None)?;
        let sorted_columns = raw_batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &sorted_indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let sorted_batch = RecordBatch::try_new(self.schema.clone(), sorted_columns)?;
        self.sink.write_batch(sorted_batch)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_batch()?;
        self.sink.close()
    }
}
