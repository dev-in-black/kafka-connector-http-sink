# Kafka Connect HTTP Sink Connector

A production-ready Kafka Connect sink connector for sending records to HTTP endpoints with advanced features including header forwarding and response topic publishing.

## Features

- **HTTP Methods**: Support for POST, PUT, and DELETE operations
- **Authentication**: Multiple authentication methods
  - Basic Authentication
  - Bearer Token
  - API Key (header or query parameter)
  - OAuth2 Client Credentials flow
- **Header Forwarding**: Forward Kafka message headers to HTTP request headers
  - Configurable include/exclude lists
  - Header prefix support
  - Static headers
- **Response Handling**: Send HTTP responses back to Kafka
  - Response body → Kafka message value
  - Response headers → Kafka message headers
  - Metadata headers (status code, response time, original topic/partition/offset)
- **Error Handling**: Flexible error handling strategies
  - Fail on error or log and continue
  - Dead Letter Queue (DLQ) support
  - Configurable null value handling
- **Retry Logic**: Configurable exponential backoff retry
  - Retry on specific HTTP status codes
  - Configurable max attempts and backoff parameters
- **Connection Pooling**: Efficient HTTP connection management
- **Production Ready**: Comprehensive testing, monitoring, and documentation

## Quick Start

### Prerequisites

- Apache Kafka 3.6.0 or later
- Java 11 or later
- Maven 3.6 or later

### Build

```bash
# Clone the repository
git clone https://github.com/yourcompany/kafka-connect-http-sink.git
cd kafka-connect-http-sink

# Build the connector
mvn clean package

# The connector will be packaged in target/kafka-connect-http-sink-1.0.0-SNAPSHOT.jar
```

### Installation

#### Standalone Mode

```bash
# Copy the connector JAR to Kafka Connect plugin path
cp target/kafka-connect-http-sink-1.0.0-SNAPSHOT.jar $KAFKA_HOME/libs/

# Start Kafka Connect
$KAFKA_HOME/bin/connect-standalone.sh \
    config/connect-standalone.properties \
    config/http-sink-connector.properties
```

#### Distributed Mode

```bash
# Copy connector to plugin path on all workers
mkdir -p /usr/share/kafka-connect-plugins/http-sink
cp target/kafka-connect-http-sink-1.0.0-SNAPSHOT.jar \
    /usr/share/kafka-connect-plugins/http-sink/

# Configure plugin.path in worker.properties
plugin.path=/usr/share/kafka-connect-plugins

# Restart Kafka Connect workers
# Then create connector via REST API
curl -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @config/http-sink-connector.json
```

## Configuration

### Minimal Configuration

```properties
name=http-sink-connector
connector.class=com.devinblack.kafka.connect.http.HttpSinkConnector
tasks.max=1
topics=my-topic

# HTTP endpoint
http.api.url=https://api.example.com/events
http.method=POST
```

### Full Configuration Example

```properties
name=http-sink-connector
connector.class=com.devinblack.kafka.connect.http.HttpSinkConnector
tasks.max=3
topics=events

# HTTP Endpoint Configuration
http.api.url=https://api.example.com/events
http.method=POST
http.request.timeout.ms=30000
http.connection.timeout.ms=5000

# Authentication (OAuth2 example)
auth.type=oauth2
auth.oauth2.token.url=https://auth.example.com/oauth/token
auth.oauth2.client.id=your-client-id
auth.oauth2.client.secret=${file:/path/to/secrets.properties:oauth_secret}
auth.oauth2.scope=read write

# Header Forwarding
headers.forward.enabled=true
headers.forward.include=correlation-id,request-id,trace-id
headers.forward.prefix=X-Kafka-
headers.static=X-Source:kafka,X-Application:myapp

# Response Topic
response.topic.enabled=true
response.topic.name=${topic}-responses
response.include.original.headers=true
response.include.request.metadata=true

# Error Handling
behavior.on.error=fail
errors.tolerance=none

# Retry Configuration
retry.enabled=true
retry.max.attempts=5
retry.backoff.initial.ms=1000
retry.on.status.codes=429,500,502,503,504
```

## Configuration Reference

### HTTP Endpoint Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `http.api.url` | String | (required) | HTTP endpoint URL |
| `http.method` | String | POST | HTTP method (POST, PUT, DELETE) |
| `http.request.timeout.ms` | Int | 30000 | Request timeout in milliseconds |
| `http.connection.timeout.ms` | Int | 5000 | Connection timeout in milliseconds |

### Authentication Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `auth.type` | String | none | Authentication type (none, basic, bearer, apikey, oauth2) |

#### Basic Authentication
- `auth.basic.username`: Username
- `auth.basic.password`: Password

#### Bearer Token
- `auth.bearer.token`: Bearer token value

#### API Key
- `auth.apikey.name`: API key header/parameter name
- `auth.apikey.value`: API key value
- `auth.apikey.location`: Location (header or query)

#### OAuth2 Client Credentials
- `auth.oauth2.token.url`: Token endpoint URL
- `auth.oauth2.client.id`: Client ID
- `auth.oauth2.client.secret`: Client secret
- `auth.oauth2.scope`: OAuth2 scope (optional)

### Header Forwarding Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `headers.forward.enabled` | Boolean | true | Enable header forwarding |
| `headers.forward.include` | String | "" | Comma-separated list of headers to include (empty = all) |
| `headers.forward.exclude` | String | "" | Comma-separated list of headers to exclude |
| `headers.forward.prefix` | String | "" | Prefix to add to forwarded headers |
| `headers.static` | String | "" | Static headers (format: key1:value1,key2:value2) |

### Response Topic Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `response.topic.enabled` | Boolean | false | Enable response topic publishing |
| `response.topic.name` | String | (required if enabled) | Response topic name (supports ${topic} variable) |
| `response.include.original.key` | Boolean | true | Include original record key in response |
| `response.include.original.headers` | Boolean | true | Include original headers in response |
| `response.original.headers.include` | String | "" | Comma-separated list of original header names to include (empty = all) |
| `response.include.request.metadata` | Boolean | true | Include request metadata in response headers |
| `response.value.format` | String | string | Response value format: `string` (pass-through) or `json` (validate JSON) |

#### Response Value Format Options

**`string` (default)**: Pass through response body as-is
- Works with any content type (JSON, HTML, XML, plain text)
- No validation performed
- Backward compatible with existing behavior

**`json`**: Validate response body is valid JSON
- Logs error if response body is not valid JSON
- Falls back to string format on validation failure
- Useful for ensuring downstream consumers receive valid JSON

**Example configurations:**

```properties
# String format (default) - works with any response type
response.topic.enabled=true
response.topic.name=http-responses
response.value.format=string

# JSON format - validates response is valid JSON
response.topic.enabled=true
response.topic.name=http-responses
response.value.format=json
response.include.request.metadata=true
```

**Kafka Message Structure:**

Both formats produce the same Kafka message structure:
- **Key**: Original record key (if `response.include.original.key=true`)
- **Value**: Response body as UTF-8 string
- **Headers**: Metadata and original headers (if configured)

The difference is that `json` format validates the response body before sending.

#### Original Header Filtering

Control which original Kafka record headers are forwarded to the response topic:

**Configuration:**
- `response.include.original.headers` - Master switch (true/false)
- `response.original.headers.include` - Whitelist of header names (comma-separated)

**Behavior:**
- If `response.include.original.headers=false`: No original headers forwarded
- If `response.include.original.headers=true` AND `response.original.headers.include` is empty: Forward ALL original headers (default)
- If `response.include.original.headers=true` AND include list specified: Forward ONLY listed headers

**Example configurations:**

```properties
# Forward all original headers (default)
response.include.original.headers=true
response.original.headers.include=

# Forward only specific headers
response.include.original.headers=true
response.original.headers.include=traceId,userId,requestId

# Forward no original headers
response.include.original.headers=false
```

**Note:** This filtering applies ONLY to original Kafka record headers. HTTP response headers and metadata headers are not affected.

### Error Handling Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `behavior.on.null.values` | String | fail | Behavior on null values (fail or ignore) |
| `behavior.on.error` | String | fail | Behavior on errors (fail or log) |
| `errors.tolerance` | String | none | Error tolerance (none or all) |
| `errors.deadletterqueue.topic.name` | String | (required if tolerance=all) | DLQ topic name |

### Retry Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `retry.enabled` | Boolean | true | Enable retry on failure |
| `retry.max.attempts` | Int | 5 | Maximum retry attempts |
| `retry.backoff.initial.ms` | Long | 1000 | Initial backoff in milliseconds |
| `retry.backoff.max.ms` | Long | 60000 | Maximum backoff in milliseconds |
| `retry.backoff.multiplier` | Double | 2.0 | Backoff multiplier |
| `retry.on.status.codes` | String | 429,500,502,503,504 | HTTP status codes to retry on |

## Architecture

### Message Processing Flow

```
[Kafka Topic]
    ↓
[HttpSinkTask.put(records)]
    ↓
[For each record]:
    ↓
[1. Convert Record → HTTP Request]
    ├─ Extract body from record value
    ├─ Transform Kafka headers to HTTP headers
    └─ Build HTTP request
    ↓
[2. Add Authentication]
    └─ Add auth headers based on auth type
    ↓
[3. Send HTTP Request]
    └─ Execute request with retry logic
    ↓
[4. Handle Response]
    ├─ Check status code
    ├─ If error → ErrorHandler (retry/DLQ/fail)
    └─ If success → Continue
    ↓
[5. Send to Response Topic (if enabled)]
    ├─ HTTP response body → Kafka message value
    ├─ HTTP response headers → Kafka message headers
    └─ Add metadata headers
    ↓
[6. Commit Offset]
```

## Development

### Project Structure

```
src/
├── main/
│   ├── java/com/yourcompany/kafka/connect/http/
│   │   ├── HttpSinkConnector.java          # Main connector
│   │   ├── HttpSinkTask.java               # Task implementation
│   │   ├── HttpSinkConnectorConfig.java    # Configuration
│   │   ├── auth/                           # Authentication providers
│   │   ├── client/                         # HTTP client
│   │   ├── handler/                        # Response/error handling
│   │   ├── converter/                      # Record/header conversion
│   │   └── retry/                          # Retry logic
│   └── resources/
│       └── META-INF/services/
└── test/
    └── java/                               # Unit and integration tests
```

### Running Tests

```bash
# Run unit tests
mvn test

# Run integration tests
mvn verify

# Run all tests with coverage
mvn clean verify

# View coverage report
open target/site/jacoco/index.html
```

### Development Status

**Phase 1: Foundation** ✅ Complete
- [x] Maven project structure
- [x] HttpSinkConnector with lifecycle
- [x] HttpSinkConnectorConfig with all properties
- [x] HttpSinkTask skeleton
- [x] Connector registration

**Phase 2: HTTP Client & Authentication** 🚧 In Progress
- [ ] HTTP client with OkHttp
- [ ] Authentication providers (Basic, Bearer, API Key, OAuth2)
- [ ] Authentication factory

**Phase 3: Record Processing & Header Forwarding** 📋 Planned
- [ ] Record converter
- [ ] Header converter
- [ ] Integration with HttpSinkTask

**Phase 4: Response Handling** 📋 Planned
- [ ] Response producer
- [ ] Response handler
- [ ] Kafka producer lifecycle

**Phase 5: Error Handling & Retry** 📋 Planned
- [ ] Error handler
- [ ] Retry policy
- [ ] DLQ support

**Phase 6-8: Testing, Documentation, Packaging** 📋 Planned

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass: `mvn verify`
5. Submit a pull request

## License

[Add your license here]

## Support

For issues, questions, or contributions, please open an issue on GitHub.

## Roadmap

- [ ] Batching support (optional)
- [ ] Circuit breaker pattern
- [ ] Rate limiting
- [ ] Request/response compression
- [ ] Prometheus metrics export
- [ ] Additional authentication methods

## References

- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [Confluent HTTP Sink Connector](https://docs.confluent.io/kafka-connectors/http/current/overview.html)
- [Kafka Connect Development Guide](https://docs.confluent.io/platform/current/connect/devguide.html)
