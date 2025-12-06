# Error Topic Feature - Quick Start

## ⚠️ Current Status
**Java is NOT installed on this system. You need to install Java to compile and test.**

## 🚀 Quick Commands (After Installing Java)

```bash
# 1. Verify Java is installed
java -version

# 2. Run the error topic tests
./gradlew test --tests "*ErrorHandlerTest"

# 3. Run all tests
./gradlew test

# 4. Build the connector
./gradlew build

# 5. View test report
open build/reports/tests/test/index.html
```

## 📦 What's Ready

### ✅ Implementation Complete
- **3 new classes** for error handling
- **2 modified files** integrated into connector
- **1 comprehensive test file** with 11 test cases
- **Configuration updated** with error topic settings

### 📁 Files Created/Modified

#### New Java Files:
1. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorRecord.java`
2. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorHandler.java`
3. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorProducer.java`

#### Modified Java Files:
1. `src/main/java/com/devinblack/kafka/connect/http/HttpSinkConnectorConfig.java`
2. `src/main/java/com/devinblack/kafka/connect/http/HttpSinkTask.java`

#### Test File:
1. `src/test/java/com/devinblack/kafka/connect/http/error/ErrorHandlerTest.java`

#### Configuration:
1. `config/http-sink-connector.properties`

## 🧪 Test Coverage

**ErrorHandlerTest.java** - 11 tests covering:
- ✅ HTTP error scenarios (4xx, 5xx)
- ✅ Retry exhaustion tracking
- ✅ Conversion error handling
- ✅ Null value handling
- ✅ Topic name resolution (static + dynamic)
- ✅ Header forwarding (original + HTTP response)
- ✅ JSON payload validation
- ✅ Metadata completeness

## ⚙️ Configuration

Add to your connector properties:

```properties
# Enable error topic
error.topic.enabled=true

# Static error topic name
error.topic.name=http-sink-errors

# OR dynamic per-source-topic naming
error.topic.name=${topic}-errors
```

## 🔍 What Errors Are Captured

When `error.topic.enabled=true`:
1. **HTTP Errors** - 4xx, 5xx status codes
2. **Retry Exhausted** - After max retry attempts
3. **Conversion Errors** - Invalid record format
4. **Null Values** - Null record values
5. **Processing Errors** - Any exception during processing

## 📊 Error Record Format

**JSON Payload:**
```json
{
  "errorType": "HTTP_ERROR",
  "errorMessage": "HTTP request returned status 500",
  "errorTimestamp": 1701728400000,
  "retryCount": 5,
  "httpStatusCode": 500,
  "httpResponseBody": "{\"error\":\"Internal Server Error\"}",
  "originalTopic": "events",
  "originalPartition": 2,
  "originalOffset": 12345
}
```

**Headers:**
- `error.type` - HTTP_ERROR, RETRY_EXHAUSTED, CONVERSION_ERROR, NULL_VALUE
- `error.message` - Error description
- `error.timestamp` - When error occurred
- `error.http.status.code` - HTTP status (if applicable)
- `error.retry.count` - Retry attempts
- `kafka.original.topic/partition/offset` - Source record location
- Original record headers (forwarded)
- HTTP response headers (prefixed with `http.response.`)

## 📝 Installation Steps

### 1. Install Java (macOS)
```bash
# Install Homebrew if needed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java
brew install openjdk@17

# Set environment
export JAVA_HOME=$(brew --prefix openjdk@17)
export PATH="$JAVA_HOME/bin:$PATH"

# Add to your ~/.zshrc or ~/.bash_profile
echo 'export JAVA_HOME=$(brew --prefix openjdk@17)' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
```

### 2. Run Tests
```bash
cd /Users/vanhc/Documents/http-sink
./gradlew test --tests "*ErrorHandlerTest"
```

### 3. Build Connector
```bash
./gradlew build
```

### 4. Deploy
```bash
# Copy JAR to Kafka Connect plugins directory
cp build/libs/http-sink-connector-*.jar /path/to/kafka/plugins/

# Restart Kafka Connect
# Configure connector with error.topic.enabled=true
```

## 🎯 Expected Results

All 11 tests should pass ✅

```
ErrorHandlerTest > testCreateErrorRecordWithHttpError() PASSED
ErrorHandlerTest > testCreateErrorRecordWithRetryExhaustion() PASSED
ErrorHandlerTest > testCreateErrorRecordWithConversionError() PASSED
ErrorHandlerTest > testCreateErrorRecordWithNullValue() PASSED
ErrorHandlerTest > testResolveErrorTopicStatic() PASSED
ErrorHandlerTest > testResolveErrorTopicDynamic() PASSED
ErrorHandlerTest > testErrorRecordIncludesOriginalHeaders() PASSED
ErrorHandlerTest > testErrorRecordIncludesHttpResponseHeaders() PASSED
ErrorHandlerTest > testErrorRecordTimestampPresent() PASSED

BUILD SUCCESSFUL
```

## 📚 Additional Resources

- Full instructions: `TEST_INSTRUCTIONS.md`
- Configuration file: `config/http-sink-connector.properties`
- Implementation plan: `.claude/plans/lexical-moseying-treehouse.md`

---

**Ready to test once Java is installed!** 🚀
