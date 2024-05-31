use std::fmt;

use vector_lib::lookup::lookup_v2::ConfigValuePath;
use vrl::path::PathPrefix;

use crate::{
    sinks::{
        infino::{
            encoder::ProcessedEvent, request_builder::InfinoRequestBuilder, service::InfinoRequest,
            BulkAction, InfinoCommonMode,
        },
        prelude::*,
    },
    transforms::metric_to_log::MetricToLog,
};

use super::{
    encoder::{DocumentMetadata, DocumentVersion, DocumentVersionType},
    InfinoCommon, InfinoConfig, VersionType,
};

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct PartitionKey {
    pub index: String,
    pub bulk_action: BulkAction,
}

pub struct InfinoSink<S> {
    pub batch_settings: BatcherSettings,
    pub request_builder: InfinoRequestBuilder,
    pub transformer: Transformer,
    pub service: S,
    pub mode: InfinoCommonMode,
    pub id_key_field: Option<ConfigValuePath>,
}

impl<S> InfinoSink<S> {
    pub fn new(common: &InfinoCommon, config: &InfinoConfig, service: S) -> crate::Result<Self> {
        let batch_settings = config.batch.into_batcher_settings()?;

        Ok(InfinoSink {
            batch_settings,
            request_builder: common.request_builder.clone(),
            transformer: config.encoding.clone(),
            service,
            mode: common.mode.clone(),
        })
    }
}

impl<S> InfinoSink<S>
where
    S: Service<InfinoRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    pub async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mode = self.mode;
        let id_key_field = self.id_key_field.as_ref();
        let transformer = self.transformer.clone();

        input
            .filter_map(move |event| {
                future::ready(match event {
                    Event::Metric(metric) => {
                        let ingest_metric = convert_metric_to_ingest_metric(
                            metric,
                            &mode,
                            id_key_field,
                            &transformer,
                        );
                        let result = state
                            .coredb
                            .append_metric_point("default", ingest_metric)
                            .await;
                        if let Err(error) = result {
                            match error {
                                CoreDBError::TooManyAppendsError() => {
                                    return Err((StatusCode::TOO_MANY_REQUESTS, error.to_string()));
                                }
                                CoreDBError::UnsupportedIngest(_)
                                | CoreDBError::IndexNotFound(_) => {
                                    return Err((StatusCode::BAD_REQUEST, error.to_string()));
                                }
                                _ => {
                                    error!("An unexpected error occurred.");
                                }
                            }
                        }
                    }
                    Event::Log(log) => {
                        let ingest_log =
                            convert_log_to_ingest_log(log, &mode, id_key_field, &transformer);
                        let result = state.coredb.append_event(ingest_log).await;
                    }
                    Event::Trace(_) => {
                        // Although technically this will cause the event to be dropped, due to the sink
                        // config it is not possible to send traces to this sink - so this situation can
                        // never occur. We don't need to emit an `EventsDropped` event.
                        None
                    }
                })
            })
            .for_each(|_| future::ready(()))
            .await;

        Ok(())
    }
}
fn convert_metric_to_ingest_metric(metric: Metric) -> IngestMetric<'_> {
    let metric_name = metric.name();
    let metric_point = MetricPoint {
        time: metric.timestamp().as_secs(),
        value: match metric.value() {
            MetricValue::Counter { value } => *value as f64,
            MetricValue::Gauge { value } => *value as f64,
            MetricValue::Histogram { samples, .. } => samples.sum(),
            MetricValue::Set { values } => values.len() as f64,
            MetricValue::Distribution { samples, .. } => samples.sum(),
        },
    };
    let labels = EventReference::new_with_params(
        metric
            .tags()
            .iter()
            .map(|(k, v)| FieldReference::new(k, v, None))
            .collect(),
    );
    IngestMetric {
        metric_name,
        metric_point,
        labels,
    }
}

fn convert_log_to_ingest_log(log: LogEvent) -> IngestLog<'_> {
    let index_name = "logs";
    let operation = IndexOperation::Index;
    let time = log.timestamp().as_secs();
    let message = EventReference::new_with_params(
        log.fields()
            .iter()
            .map(|(k, v)| FieldReference::new(k, v.as_str().unwrap_or(""), None))
            .collect(),
    );
    IngestLog {
        index_name,
        operation,
        time,
        message,
    }
}

#[async_trait]
impl<S> StreamSink<Event> for InfinoSink<S>
where
    S: Service<InfinoRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
