use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
};

use futures::{FutureExt, TryFutureExt};
use vector_lib::configurable::configurable_component;

use crate::{
    codecs::Transformer,
    config::{AcknowledgementsConfig, DataType, Input, SinkConfig, SinkContext},
    event::{EventRef, LogEvent, Value},
    internal_events::TemplateRenderingError,
    sinks::{
        util::{
            http::RequestConfig, service::HealthConfig, BatchConfig, Compression,
            RealtimeSizeBasedDefaultBatchSettings,
        },
        Infino::{
            retry::InfinoRetryLogic,
            service::{HttpRequestBuilder, InfinoService},
            sink::InfinoSink,
            InfinoApiVersion, InfinoAuthConfig, InfinoCommon, InfinoCommonMode, InfinoMode,
            VersionType,
        },
        VectorSink,
    },
    template::Template,
    tls::TlsConfig,
    transforms::metric_to_log::MetricToLogConfig,
};
use vector_lib::lookup::event_path;
use vector_lib::lookup::lookup_v2::ConfigValuePath;
use vector_lib::schema::Requirement;
use vrl::value::Kind;

/// The field name for the timestamp required by data stream mode
pub const DATA_STREAM_TIMESTAMP_KEY: &str = "@timestamp";

/// Configuration for the `Infino` sink.
#[configurable_component(sink("Infino", "Index observability events in Infino."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct InfinoConfig {
    /// Whether or not to retry successful requests containing partial failures.
    ///
    /// To avoid duplicates in Infino, please use option `id_key`.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub request_retry_partial: bool,

    /// The name of the pipeline to apply.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::examples = "pipeline-name"))]
    pub pipeline: Option<String>,

    #[serde(default)]
    #[configurable(derived)]
    pub compression: Compression,

    #[serde(skip_serializing_if = "crate::serde::is_default", default)]
    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    pub encoding: Transformer,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    #[configurable(derived)]
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for InfinoConfig {
    fn default() -> Self {
        Self {
            request_retry_partial: false,
            pipeline: None,
            encoding: Default::default(),
            acknowledgements: Default::default(),
        }
    }
}

impl InfinoConfig {
    pub fn common_mode(&self) -> crate::Result<InfinoCommonMode> {
        match self.mode {
            InfinoMode::Bulk => Ok(InfinoCommonMode::Bulk {
                index: self.bulk.index.clone(),
                action: self.bulk.action.clone(),
                version: self.bulk.version.clone(),
                version_type: self.bulk.version_type,
            }),
            InfinoMode::DataStream => Ok(InfinoCommonMode::DataStream(
                self.data_stream.clone().unwrap_or_default(),
            )),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "Infino")]
impl SinkConfig for InfinoConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<VectorSink> {
        let commons = InfinoCommon::parse_many(self, cx.proxy()).await?;
        let common = commons[0].clone();

        let request_limits = self.request.tower.into_settings();

        let sink = InfinoSink::new(&common, self, service)?;

        let stream = VectorSink::from_event_streamsink(sink);

        Ok(stream)
    }

    fn input(&self) -> Input {
        let requirements = Requirement::empty().optional_meaning("timestamp", Kind::timestamp());

        Input::new(DataType::Metric | DataType::Log).with_schema_requirement(requirements)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<InfinoConfig>();
    }

    #[test]
    fn parse_aws_auth() {
        toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
            auth.strategy = "aws"
            auth.assume_role = "role"
        "#,
        )
        .unwrap();

        toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
            auth.strategy = "aws"
        "#,
        )
        .unwrap();
    }

    #[test]
    fn parse_mode() {
        let config = toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
            mode = "data_stream"
            data_stream.type = "synthetics"
        "#,
        )
        .unwrap();
        assert!(matches!(config.mode, InfinoMode::DataStream));
        assert!(config.data_stream.is_some());
    }

    #[test]
    fn parse_distribution() {
        toml::from_str::<InfinoConfig>(
            r#"
            endpoints = ["", ""]
            distribution.retry_initial_backoff_secs = 10
        "#,
        )
        .unwrap();
    }

    #[test]
    fn parse_version() {
        let config = toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
            api_version = "v7"
        "#,
        )
        .unwrap();
        assert_eq!(config.api_version, InfinoApiVersion::V7);
    }

    #[test]
    fn parse_version_auto() {
        let config = toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
            api_version = "auto"
        "#,
        )
        .unwrap();
        assert_eq!(config.api_version, InfinoApiVersion::Auto);
    }

    #[test]
    fn parse_default_bulk() {
        let config = toml::from_str::<InfinoConfig>(
            r#"
            endpoints = [""]
        "#,
        )
        .unwrap();
        assert_eq!(config.mode, InfinoMode::Bulk);
        assert_eq!(config.bulk, BulkConfig::default());
    }
}
