# Error Topic Feature - Testing Instructions

## Prerequisites

Install Java JDK (required to compile and test):

```bash
# macOS - Install Homebrew first if not installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Then install Java
brew install openjdk@17

# Set JAVA_HOME
export JAVA_HOME=$(brew --prefix openjdk@17)
export PATH="$JAVA_HOME/bin:$PATH"

# Verify installation
java -version
```

## Running Tests

### Run All Tests
```bash
./gradlew test
```

### Run Only Error Topic Tests
```bash
./gradlew test --tests "*ErrorHandlerTest"
```

### Run Tests with Detailed Output
```bash
./gradlew test --tests "*ErrorHandlerTest" --info
```

### Run Specific Test
```bash
./gradlew test --tests "*ErrorHandlerTest.testCreateErrorRecordWithHttpError"
```

### Build Project (Compile Only)
```bash
./gradlew build -x test
```

### Clean and Build
```bash
./gradlew clean build
```

## Test Reports

After running tests, view the HTML report:
```bash
open build/reports/tests/test/index.html
```

Or find it at:
```
build/reports/tests/test/index.html
```

## What Was Implemented

### New Files Created (3):
1. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorRecord.java`
2. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorHandler.java`
3. `src/main/java/com/devinblack/kafka/connect/http/error/ErrorProducer.java`

### New Test File Created (1):
1. `src/test/java/com/devinblack/kafka/connect/http/error/ErrorHandlerTest.java` - 11 test cases

### Modified Files (2):
1. `src/main/java/com/devinblack/kafka/connect/http/HttpSinkConnectorConfig.java` - Added error topic config
2. `src/main/java/com/devinblack/kafka/connect/http/HttpSinkTask.java` - Integrated error handling

### Configuration File Updated (1):
1. `config/http-sink-connector.properties` - Added ERROR TOPIC CONFIGURATION section

## Test Coverage

### ErrorHandlerTest.java - 11 Tests:
1. ✅ `testCreateErrorRecordWithHttpError` - HTTP 500 error with response body
2. ✅ `testCreateErrorRecordWithRetryExhaustion` - Retry exhaustion with count
3. ✅ `testCreateErrorRecordWithConversionError` - Conversion failures
4. ✅ `testCreateErrorRecordWithNullValue` - Null value handling
5. ✅ `testResolveErrorTopicStatic` - Static topic name
6. ✅ `testResolveErrorTopicDynamic` - ${topic} variable substitution
7. ✅ `testErrorRecordIncludesOriginalHeaders` - Original headers forwarding
8. ✅ `testErrorRecordIncludesHttpResponseHeaders` - HTTP response headers with prefix
9. ✅ `testErrorRecordTimestampPresent` - Timestamp generation
10. ✅ All error types covered: HTTP_ERROR, RETRY_EXHAUSTED, CONVERSION_ERROR, NULL_VALUE
11. ✅ Header metadata validation

## Configuration Examples

### Enable Error Topic (Static Name)
```properties
error.topic.enabled=true
error.topic.name=http-sink-errors
```

### Enable Error Topic (Dynamic Per Source Topic)
```properties
error.topic.enabled=true
error.topic.name=${topic}-errors
```

### Full Configuration Example
```properties
# Enable error topic publishing
error.topic.enabled=true
error.topic.name=http-sink-errors

# HTTP endpoint
http.api.url=https://api.example.com/events

# When error.topic.enabled=true, these are overridden:
behavior.on.error=fail  # Ignored when error topic enabled
behavior.on.null.values=fail  # Ignored when error topic enabled

# Errors are sent to error topic and processing continues
```

## Expected Test Results

All 11 tests should pass:
- ✅ Error record creation with HTTP errors
- ✅ Retry exhaustion tracking
- ✅ Conversion error handling
- ✅ Null value error handling
- ✅ Topic name resolution (static and dynamic)
- ✅ Header forwarding (original + HTTP response)
- ✅ JSON payload validation
- ✅ Metadata completeness

## Troubleshooting

### If tests fail due to missing Java:
```bash
# Verify Java is installed and in PATH
java -version
echo $JAVA_HOME

# If not set, export JAVA_HOME
export JAVA_HOME=/usr/local/opt/openjdk@17
```

### If Gradle wrapper fails:
```bash
# Make gradlew executable
chmod +x gradlew

# Use wrapper
./gradlew --version
```

### If compilation fails:
```bash
# Clean and rebuild
./gradlew clean
./gradlew build --info
```

## Next Steps

1. Install Java JDK 17+
2. Run `./gradlew test` to verify all tests pass
3. Build the connector: `./gradlew build`
4. Deploy to Kafka Connect cluster
5. Configure with error topic enabled
6. Monitor error topic for captured errors
