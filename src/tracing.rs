//! Distributed tracing support using OpenTelemetry.
//!
//! Provides trace context propagation across services for debugging and observability.

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for distributed tracing.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Whether tracing is enabled
    pub enabled: bool,
    /// OTLP endpoint for trace export (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Service name for traces
    pub service_name: String,
    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: None,
            service_name: "arcrun".to_string(),
            sampling_ratio: 1.0,
        }
    }
}

impl TracingConfig {
    /// Load tracing configuration from environment variables.
    ///
    /// Environment variables:
    /// - `TRACING_ENABLED`: Enable distributed tracing (default: false)
    /// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL
    /// - `OTEL_SERVICE_NAME`: Service name (default: arcrun)
    /// - `OTEL_SAMPLING_RATIO`: Sampling ratio 0.0-1.0 (default: 1.0)
    pub fn from_env() -> Self {
        let enabled = std::env::var("TRACING_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "arcrun".to_string());

        let sampling_ratio = std::env::var("OTEL_SAMPLING_RATIO")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.0);

        Self {
            enabled,
            otlp_endpoint,
            service_name,
            sampling_ratio,
        }
    }
}

/// Initialize the tracing subscriber with OpenTelemetry integration.
///
/// If tracing is disabled, only sets up the log layer.
/// If tracing is enabled, sets up both logging and OTLP trace export.
pub fn init_tracing(config: &TracingConfig) -> Option<SdkTracerProvider> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    if !config.enabled {
        // Just logging, no OTLP export
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        log::info!("Tracing initialized (OTLP disabled)");
        return None;
    }

    // Build OTLP exporter if endpoint is configured
    let Some(endpoint) = &config.otlp_endpoint else {
        log::warn!("TRACING_ENABLED=true but OTEL_EXPORTER_OTLP_ENDPOINT not set, disabling OTLP");
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        return None;
    };

    // Build the OTLP exporter
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            log::error!("Failed to create OTLP exporter: {}, disabling tracing", e);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();
            return None;
        }
    };

    // Build tracer provider with sampling
    let sampler = opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(config.sampling_ratio);
    let resource = opentelemetry_sdk::Resource::builder()
        .with_attributes(vec![KeyValue::new(
            "service.name",
            config.service_name.clone(),
        )])
        .build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    let tracer = tracer_provider.tracer("arcrun");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    log::info!(
        "Tracing initialized with OTLP export to {} (service={}, sampling={})",
        endpoint,
        config.service_name,
        config.sampling_ratio
    );

    Some(tracer_provider)
}

/// Shutdown the tracer provider gracefully.
pub fn shutdown_tracing(provider: Option<SdkTracerProvider>) {
    if let Some(provider) = provider
        && let Err(e) = provider.shutdown()
    {
        log::error!("Error shutting down tracer provider: {:?}", e);
    }
}

/// Extract trace context from HTTP headers.
///
/// Supports W3C Trace Context format (traceparent, tracestate headers).
pub fn extract_trace_context(
    headers: &actix_web::http::header::HeaderMap,
) -> opentelemetry::Context {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    let propagator = TraceContextPropagator::new();
    let extractor = HeaderExtractor(headers);
    propagator.extract(&extractor)
}

/// Inject trace context into HTTP headers.
///
/// Adds W3C Trace Context headers (traceparent, tracestate) for propagation.
pub fn inject_trace_context(headers: &mut reqwest::header::HeaderMap, cx: &opentelemetry::Context) {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    let propagator = TraceContextPropagator::new();
    let mut injector = HeaderInjector(headers);
    propagator.inject_context(cx, &mut injector);
}

/// Header extractor for actix-web HeaderMap
struct HeaderExtractor<'a>(&'a actix_web::http::header::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Header injector for reqwest HeaderMap
struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
            && let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value)
        {
            self.0.insert(header_name, header_value);
        }
    }
}

/// Get the current trace ID as a string, if available.
///
/// Returns the trace ID in hex format, or None if not in a traced context.
pub fn current_trace_id() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::Span::current();
    let cx = span.context();
    let span_ref = cx.span();
    let trace_id = span_ref.span_context().trace_id();

    if trace_id == opentelemetry::trace::TraceId::INVALID {
        None
    } else {
        Some(trace_id.to_string())
    }
}

/// Get the current span ID as a string, if available.
pub fn current_span_id() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::Span::current();
    let cx = span.context();
    let span_ref = cx.span();
    let span_id = span_ref.span_context().span_id();

    if span_id == opentelemetry::trace::SpanId::INVALID {
        None
    } else {
        Some(span_id.to_string())
    }
}
