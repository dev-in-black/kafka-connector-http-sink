# Confluent Hub Component Archive Specification

This project has been adapted to comply with the [Confluent Hub Component Archive Specification](https://docs.confluent.io/platform/current/connect/confluent-hub/component-archive.html).

## Directory Structure

The archive follows the required structure:

```
devinblack-kafka-connect-http-sink-{version}.zip
├── manifest.json               # Component metadata
├── doc/                       # Documentation
│   ├── LICENSE               # Apache License 2.0
│   ├── README.md             # Project documentation
│   └── version.txt           # Version information
├── lib/                       # Runtime dependencies and connector JAR
│   ├── kafka-connect-http-sink-{version}.jar
│   ├── okhttp-{version}.jar
│   ├── jackson-databind-{version}.jar
│   └── guava-{version}.jar
├── etc/                       # Sample configuration files
│   └── http-sink-connector.properties
└── assets/                    # Logo and images (no subdirectories)
    └── logo.png              # Optional: 400x200 pixels minimum
```

## Building Confluent Hub Archive

### Using Gradle (Recommended)

```bash
# Build the Confluent Hub compliant archive
./gradlew createConfluentArchive

# Output location
# build/confluent/devinblack-kafka-connect-http-sink-{version}.zip
```

### Using Maven

```bash
# Build the Confluent Hub compliant archive
mvn clean package

# Output location
# target/devinblack-kafka-connect-http-sink-{version}.zip
```

## Manifest.json

The `manifest.json` includes all required fields:

- **name**: kafka-connect-http-sink
- **version**: Automatically synced from VERSION file
- **title**: HTTP Sink Connector
- **description**: Detailed component description
- **owner**: User/organization information
- **component_types**: ["sink"]
- **support**: Provider information with provider_name
- **license**: Apache License 2.0
- **features**: Supported encodings, transforms, and integrations
- **documentation_url**: GitHub README link

## Version Management

The version is managed centrally in the `VERSION` file at the project root. Both build systems read from this file to ensure consistency.

Current version: `v0.1.3`

## Required Files

### doc/LICENSE
Full Apache License 2.0 text

### doc/README.md
Complete project documentation

### doc/version.txt
Version identifier matching the VERSION file

### manifest.json
Component metadata at archive root

### etc/
Configuration examples and templates

### lib/
All runtime dependencies (excluding Kafka Connect framework libraries like connect-api, connect-runtime, kafka-clients, and slf4j which are provided)

### assets/
Logo and image files only (no subdirectories allowed)

## Archive Naming Convention

Archives follow the Confluent Hub naming convention:
```
${componentOwner}-${componentName}-${componentVersion}.zip
```

Example: `devinblack-kafka-connect-http-sink-0.1.3.zip`

## Excluded Dependencies

The following dependencies are marked as "provided" and excluded from the lib/ directory:
- org.apache.kafka:connect-api
- org.apache.kafka:connect-runtime
- org.apache.kafka:kafka-clients
- org.slf4j:slf4j-api

These are provided by the Kafka Connect framework at runtime.

## Testing the Archive

After building, you can verify the archive structure:

```bash
# Extract and inspect
unzip -l build/confluent/devinblack-kafka-connect-http-sink-*.zip

# Expected structure should match the specification above
```

## Publishing to Confluent Hub

Once the archive is built and validated:

1. Create an account at https://www.confluent.io/hub/
2. Submit your connector component
3. Upload the generated ZIP file
4. Confluent will review and publish your connector

## References

- [Confluent Hub Component Archive Specification](https://docs.confluent.io/platform/current/connect/confluent-hub/component-archive.html)
- [Confluent Hub](https://www.confluent.io/hub/)
- [Kafka Connect Maven Plugin](https://docs.confluent.io/kafka-connect-maven-plugin/current/index.html)
