# Cassandra to Cloud Bigtable Proxy Adaptor

## Current Released Version

Version `1.1.0` - Release on `TBD`

For more details on this release, please refer to the [CHANGELOG.md](./CHANGELOG.md).

## Introduction

Cassandra to Cloud Bigtable Proxy Adaptor is designed to forward your application's CQL traffic to Bigtable database service. It listens on a local address and securely forwards that traffic.

## Table of Contents

- [When to use Cassandra to Bigtable Proxy?](#when-to-use-cassandra-to-bigtable-proxy)
- [Pre-requisites](#pre-requisites)
- [Authenticate and Authorize with Google Cloud](#authenticate-and-authorize-with-google-cloud)
- [Setting Up Bigtable Instance and Schema Configuration](#setting-up-bigtable-instance-and-schema-configuration)
  - [DDL Support for Schema Creation](#ddl-support-for-schema-creation-recommended-method)
  - [Manual Setup](#manual-setup-legacy-method)
- [Proxy Configuration: YAML Configuration Explained](#proxy-configuration-yaml-configuration-explained)
- [Getting started](#getting-started)
  - [Build and Run Proxy Locally](#build-and-run-proxy-locally)
  - [Run a Cassandra-to-Bigtable Proxy via Docker](#run-a-cassandra-to-bigtable-proxy-via-docker)
- [CQLSH support with Proxy](#cqlsh-support-with-proxy)
- [Limitations for Proxy Application](#limitations-for-proxy-application)
- [Guidelines for Proxy Application](#guidelines-for-proxy-application)
- [Setting Systemd Setup](#run-a-cassandra-to-bigtable-proxy-via-systemd)
- [Instrument with OpenTelemetry](#instrument-with-openTelemetry)
- [Differences from Cassandra](./docs/differences_from_cassandra.md)
- [Testing](#testing)
  - [Unit Tests](#unit-tests)
  - [Compliance Tests](#compliance-tests)
  - [Integration Tests](#integration-tests)
  - [Fuzzing Tests](#fuzzing-tests)
- [Generating Mock Files Using Mockery](./mocks/README.md)
- [Connection Methods](#connection-methods)
  - [Plain TCP Connection](#plain-tcp-connection)
  - [Secured TCP Connection (TLS)](#secured-tcp-connection-tls)
  - [Unix Domain Socket (UDS) Connection](#unix-domain-socket-uds-connection)
- [Supported Cassandra Versions](#supported-cassandra-versions)

## When to use Cassandra to Bigtable Proxy?

`cassandra-to-bigtable-proxy` enables applications that are currently using Apache Cassandra or DataStax Enterprise (DSE) and would like to switch to use Cloud Bigtable. This Proxy Adaptor can be used as Plug-N-Play for the Client Application without the need of any code changes in the Client Application.

## Pre-requisites

- Go v1.22
- Cloud Bigtable Database Instance
- Docker(Optional)
- GCP Service Account with Read/Write Permissions to Cloud Bigtable

Additionally,

You will need a [Google Cloud Platform Console][developer-console] project with the Cloud Bigtable [API enabled][enable-api].
You will need to [enable billing][enable-billing] to use Google Cloud Bigtable.
[Follow these instructions][create-project] to get your project set up.

## Authenticate and Authorize with Google Cloud
To run the **Cassandra-to-Bigtable Proxy**, you must be **authenticated and authorized** to make API calls to Google Cloud Bigtable.
There are **two primary ways** to authenticate, each suited for different environments:

1.  **Using `gcloud auth login` and a Service Account Key** *(Recommended for servers/production)*
2.  **Using `gcloud auth application-default login`** *(Recommended for local development)*

---

### Option 1: Authenticate Using `gcloud auth login` and a Service Account Key

**Use this method when running the proxy on a VM, server, or in a production environment.**

This method involves downloading a Service Account JSON key file and setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to its location.

**Steps:**

1.  **Create a Service Account:**
    * In the Google Cloud Console, navigate to IAM & Admin > Service Accounts.
    * Create a new service account with the necessary Cloud Bigtable Read/Write permissions.
2.  **Download the Service Account Key:**
    * After creating the service account, generate a JSON key file and download it to your local machine or server.
3.  **Authenticate with `gcloud auth login`:**
    ```bash
    gcloud auth login
    ```
4.  **Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:**
    * Replace `/path/to/your/service-account.json` with the actual path to your downloaded JSON key file.
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account.json"
    ```
    * **Important:** For persistent environment variable settings, add this line to your shell's configuration file (e.g., `~/.bashrc`, `~/.zshrc`).
5.  **Set the `GCLOUD_PROJECT` environment variable:**
    ```bash
    gcloud config set project [YOUR_PROJECT_ID]
    ```

---

### Option 2: Authenticate Using `gcloud auth application-default login`

**Use this method for local development and testing.**

This method uses your Google Cloud user account credentials, which are typically obtained via the `gcloud` CLI.

**Steps:**

1.  **Authenticate with `gcloud auth application-default login`:**
    ```bash
    gcloud auth application-default login
    ```
    * This command will open a browser window, prompting you to log in to your Google Cloud account.
    * After successful login, your credentials will be stored in the Application Default Credentials (ADC) location.
2.  **Set the `GCLOUD_PROJECT` environment variable:**
    ```bash
    gcloud config set project [YOUR_PROJECT_ID]
    ```
    * **Note:** When using this method, you **do not** need to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. The ADC automatically handles authentication.

---
## Setting Up Bigtable Instance and Schema Configuration
Before starting the `cassandra-to-bigtable-proxy` Adaptor, it's essential to set up the necessary infrastructure within Cloud Bigtable. This involves creating a Bigtable instance and configuring the schema using DDL commands.

The `schema_mapping` table acts as a metadata repository, holding the schema configuration for your Cassandra-like tables in Bigtable. It stores details such as column names, data types, and primary key information.

**Setup Process:**

There are two primary methods for setting up your Bigtable infrastructure:

### **DDL Support for Schema Creation** (Recommended Method)

The proxy now supports Data Definition Language (DDL) operations, making it easier to create and manage tables directly through CQL commands. This is the recommended approach for setting up your schema.

**Setup Steps:**

1. **Create Bigtable Instance:**
   - Create a Bigtable instance manually through the Google Cloud Console or using `gcloud` commands.
   - [Creating a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
   - Make sure to create an app profile in the bigtable instance if needed.

2. **Configure Proxy:**
   - Update `config.yaml` with your Bigtable instance details and other configuration parameters.
   - The proxy will automatically create the `schema_mapping` table if it doesn't exist.

3. **Start the Proxy:**
   ```bash
   ./cassandra-to-bigtable-proxy
   ```
   - The proxy will start with an empty cache since no tables exist yet.

4. **Create Tables Using CQLSH:**
   - Connect to the proxy using CQLSH:
     ```bash
     cqlsh localhost 9042
     ```
   - Create tables using standard Cassandra CQL syntax:
     ```sql
     CREATE TABLE keyspace.table (
         id bigint,
         name text,
         age int,
         PRIMARY KEY ((id), name)
     );
     ```
   - The proxy will automatically:
     - Create the table in Bigtable
     - Set up necessary column families
     - Populate the schema_mapping table with the configuration

5. **Batch Table Creation:**
   - You can create multiple tables at once using a SQL file:
     ```bash
     cqlsh localhost 9042 -f schema.sql
     ```
   - The `schema.sql` file can contain multiple CREATE TABLE statements.

6. **Table Modifications:**
   - The proxy supports ALTER TABLE and DROP TABLE operations:
     ```sql
     -- Add a new column
     ALTER TABLE keyspace.table ADD email text;

     -- Drop a table
     DROP TABLE keyspace.table;
     ```

**Benefits of DDL Support:**
- Simplified schema management using familiar CQL syntax
- Automatic schema_mapping table population
- Support for table modifications
- Batch table creation through SQL files
- No need for manual schema_mapping table management

### **Manual Setup** (Legacy Method)

If you prefer to manually create your Bigtable infrastructure, follow these steps:

**Prerequisites for Manual Setup:**

* You must have a Google Cloud project with the Cloud Bigtable API enabled.
* You must have the `gcloud` CLI installed and configured with appropriate credentials.
* You must have the `cbt` CLI tool installed and configured.

**Steps for Manual Setup:**

**a. Create a Bigtable Instance:**

* Create a new Bigtable instance with your desired configuration. Refer to the official Google Cloud documentation for detailed instructions:
    * [Creating a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
* Make sure to create an app profile in the bigtable instance if needed.

**b. Create the `schema_mapping` Table:**

* Ensure you are logged in to your Google Cloud account before executing these commands.
* Use the `cbt` CLI to create the `schema_mapping` table and its column family.

    ```bash
    cbt -project GCP_PROJECT_ID -instance BIGTABLE_INSTANCE createtable schema_mapping
    cbt -project GCP_PROJECT_ID -instance BIGTABLE_INSTANCE createfamily schema_mapping cf
    ```

    * Replace `GCP_PROJECT_ID` with your Google Cloud project ID and `BIGTABLE_INSTANCE` with the name of your Bigtable instance.

**c. Create Other Tables and Populate `schema_mapping`:**

* Create all other tables that your application will use, along with their respective column families and columns.
* For each table and its columns, insert corresponding entries into the `schema_mapping` table. This step is crucial for the proxy to correctly map Cassandra-like queries to Bigtable.
* For more information on managing tables please refer to the official documentation:
    * [Creating and Managing Tables](https://cloud.google.com/bigtable/docs/managing-tables)

## Proxy Configuration: YAML Configuration Explained

The `cassandra-to-bigtable-proxy` Adaptor is configured using the `config.yaml` file located in the root directory of the application. This file allows you to specify various settings, including listener configurations, Bigtable connection details, open telemetry configurations and logging options.

Below is a detailed breakdown of the configuration variables:

```yaml
cassandraToBigtableConfigs:
  # [Optional] Global default GCP Project ID
  projectId: YOUR_GCP_PROJECT
  # [Optional] Global default configuration table name
  schemaMappingTable: SCHEMA_MAPPING_TABLE_NAME
  #[Required] Default column family for primitive data types
  defaultColumnFamily: DEFAULT_COLUMN_FAMILY

listeners:
- name: YOUR_CLUSTER_NAME
  port: PORT_NUMBER
  bigtable:
    # [Optional], skip if it is the same as global_projectId.
    projectId: YOUR_GCP_PROJECT

    # If you want to use multiple instances then pass the instance names by comma separated
    # Instance name should not contain any special characters except underscore(_)
    instanceIds: YOUR_BIGTABLE_INSTANCE_ID

    # [Required] Name of the table  where cassandra schema to bigtable schema mapping is stored.
    schemaMappingTable: SCHEMA_MAPPING_TABLE_NAME

    #[Optional] appProfileID is required only if you want to use a specific app profile.
    appProfileID: YOUR_BIGTABLE_PROFILE_ID

    #[Required] Default column family for primitive data types is "cf1"
    defaultColumnFamily: DEFAULT_COLUMN_FAMILY

    # Number of grpc channels to be used for Bigtable session.
    Session:
      grpcChannels: 4

otel:
  # Set enabled to true or false for OTEL metrics/traces/logs.
  enabled: False

  # Set enabled to true or false for client side OTEL (metrics/traces).
  enabledClientSideMetrics: True

  # Name of the collector service to be setup as a sidecar
  serviceName: YOUR_OTEL_COLLECTOR_SERVICE_NAME

  healthcheck:
    # Enable the health check in this proxy application config only if the
    # "health_check" extension is added to the OTEL collector service configuration.
    #
    # Recommendation:
    # Enable the OTEL health check if you need to verify the collector's availability
    # at the start of the application. For development or testing environments, it can
    # be safely disabled to reduce complexity.

    # Enable/Disable Health Check for OTEL, Default 'False'.
    enabled: False
    # Health check endpoint for the OTEL collector service
    endpoint: OTEL_HEALTH_CHECK_ENDPOINT
  metrics:
    # Collector service endpoint
    endpoint: YOUR_OTEL_COLLECTOR_SERVICE_ENDPOINT

  traces:
    # Collector service endpoint
    endpoint: YOUR_OTEL_COLLECTOR_SERVICE_ENDPOINT
    # Sampling ratio should be between 0 and 1. Here 0.05 means 5/100 Sampling ratio.
    samplingRatio: 1

loggerConfig:
  # Specifies the type of output, here it is set to 'file' indicating logs will be written to a file.
  # Value of `outputType` should be `file` for file type or `stdout` for standard output.
  # Default value is `stdout`.
  outputType: stdout
  # Set this only if the outputType is set to `file`.
  # The path and name of the log file where logs will be stored. For example, output.log, Required Key.
  # Default `/var/log/cassandra-to-spanner-proxy/output.log`.
  fileName: output/output.log
  # Set this only if the outputType is set to `file`.
  # The maximum size of the log file in megabytes before it is rotated. For example, 500 for 500 MB.
  maxSize: 10
  # Set this only if the outputType is set to `file`.
  # The maximum number of backup log files to keep. Once this limit is reached, the oldest log file will be deleted.
  maxBackups: 2
  # Set this only if the outputType is set to `file`.
  # The maximum age in days for a log file to be retained. Logs older than this will be deleted. Required Key.
  # Default 3 days
  maxAge: 1

  # Set this only if the outputType is set to `file`.
  # Default value is set to 'False'. Change the value to 'True', if log files are required to be compressed.
  compress: True
  ```

**Important Note:**
* These configurations are essential and must be configured correctly before starting the proxy adaptor. Incorrect configurations can lead to connection failures or unexpected behavior.
* Ensure that you replace the placeholder values (e.g., YOUR_GCP_PROJECT, PORT_NUMBER) with your actual configuration settings.
* The defaultColumnFamily must be specified.
* If you have created application profile then you can specify otherwise comment out it in `config.yaml` so that it will pick `default` column family automatically
* When using multiple bigtable instances, ensure that the schema mapping table is available in all the bigtable instances.
* When using OTEL, ensure that the OTEL collector service is configured correctly.
* If you modify any value in the config.yaml file, you must restart the proxy adaptor for the changes to take effect.

## Getting started

We can setup the `cassandra-to-bigtable-proxy` adaptor via 3 different methods as mentioned below

- Locally build and run `cassandra-to-bigtable-proxy`
- Run a docker image that has `cassandra-to-bigtable-proxy` installed
- Use a Kubernetes container to run `cassandra-to-bigtable-proxy`

### Build and Run Proxy Locally

Steps to run the adaptor locally are as mentioned below:

- Clone the repository (https://github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy.git)
- Update `config.yaml`
  #### There are several ways to run/start proxy(Using Build and without Build):
  **1. Using Build**
    ```ssh
    go build -o cassandra-to-bigtable-proxy .
    // Compiles for your current OS & architecture (e.g., Mac M1 = darwin/arm64)

    GOOS=linux GOARCH=amd64 go build -o cassandra-to-bigtable-proxy .
    // Cross-compiles for Linux (x86_64) (for running on a Linux server or VM)

    GOOS=darwin GOARCH=arm64 go build -o cassandra-to-bigtable-proxy .
    // Cross-compiles for Mac M1/M2 (ARM64)

    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cassandra-to-bigtable-proxy .
    // Builds a static binary for Linux x86_64 (portable, no external C dependencies)
    ```

  *Once you create the build then execute the below command to run the application*
  ```sh
  ./cassandra-to-bigtable-proxy
  ```

  **2. Without Build**
  - You can start proxy directly without creating build, execute the below command to start the proxy.
  ```sh
  go run proxy.go
  ```

- Optional Arguments with the default values.
  - **cql-version**: CQL version, **default value: "3.4.5"**
  - **partitioner**: Partitioner, **default value: "org.apache.cassandra.dht.Murmur3Partitioner"**
  - **release-version**: Release Version, **default value: "4.0.0.6816"**
  - **data-center**: Data center, **default value: "datacenter1"**
  - **protocol-version**: Initial protocol version to use when connecting to the backend cluster, **default value: v4**
  - **log-level**: , set log level - [info/debug/error/warn] **default value: info**
  - **tcp-bind-port** or **-t**: TCP port to bind to, **default value: 9042**
  - **use-unix-socket** or **-u**: Enable Unix Domain Socket (UDS) connection. See [UDS Connection](#unix-domain-socket-uds-connection) for details.
  - **unix-socket-path**: Path for the Unix Domain Socket file, **default value: "/tmp/cassandra-proxy.sock"**

- Run the Application with the arguments example
  ```sh
  ./cassandra-to-bigtable-proxy --log-level error -t 9043
  ```

- Application will be listening on the specified TCP port (default: 9042).

### Run a Cassandra-to-Bigtable Proxy via Docker

- Build docker image
  ```sh
    docker build --no-cache -t cassandra-to-bigtable-proxy:local .
  ```

- Start docker container using generated image

  ```sh
    docker run -d --name cassandra-bigtable-proxy \
    -p 9042:9042 \
    -e GOOGLE_APPLICATION_CREDENTIALS="/var/run/secret/cloud.google.com/bigtable-adaptor-service-account.json" \
    -v <<path to service account>>/bigtable-adaptor-service-account.json:/var/run/secret/cloud.google.com/ \
    cassandra-to-bigtable-proxy:local
  ```
### Use Kubernetes

- [Deployment via k8](https://github.com/ollionorg/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/blob/k8ssetupdocumentation/deployment/k8/README.md)

## CQLSH support with Proxy

- User can connect and use cqlsh with proxy. Detailed document - [cqlsh.md](./docs/cqlsh.md)

## Limitations for Proxy Application

Detailed document - [Limitations](./docs/limitations.md)

## Guidelines for Proxy Application

- The Proxy Adaptor supports DML operations such as INSERT, DELETE, UPDATE and SELECT.
- To run the Raw DML queries, it is mandatory for all values except numerics to have single quotes added to it. For eg.

  ```sh
  SELECT * FROM keyspace.table WHERE name='john doe';

  INSERT INTO keyspace.table (id, name) VALUES (1, 'john doe');
  ```

- Before running the proxy application, stop the Cassandra service as both run on the same port 9042.

- All the required tables should be created on Cloud Bigtable and its schema should be updated in schema_mapping table before running the Proxy Adaptor.

- Bigtable does not allow conditional WRITES operations. Currently, for UPDATE and DELETE operations, we accept columns that are part of the primary key and help us construct the rowkey that is required for these operations.

- For select operation, we can apply the conditions on the columns which are the type of string and INT64, the support for other datatypes has not been added to bigtable yet.

  ```python
  import time
  from cassandra import ConsistencyLevel
  from cassandra.cluster import Cluster, BatchStatement

  cluster = Cluster(['<<IP address>>'], port=<port>, protocol_version=4, connect_timeout=10)
  session = cluster.connect(keyspace='<<keyspace_name>>', wait_for_all_pools=True)
  session.default_consistency_level=ConsistencyLevel.LOCAL_QUORUM

  # Insert block
  pr = session.prepare("insert into keyspace.table ( guid, name, age, gender ) VALUES ( ?, ?, ?, ?);")
  rs = session.execute(pr, ("XXYYZZAAA",time.time(), 12, "male" ))
  print("Insert Successful")

  # Select block
  pr = session.prepare("select * from keyspace.table where guid = ?;")
  rs = session.execute(pr, ("XXYYZZAA",))
  print("selected rows - ", rs.all())

   # Update block
  pr = session.prepare("update keyspace.table set age=? where guid=?;")
  rs = session.execute(pr, (15, "XXYYZZAA",))
  print("Update Successful")

  # Delete block
  pr = session.prepare("delete from keyspace.table where guid = ?;")
  rs = session.execute(pr, ("XXYYZZAA",))
  print("Delete Successful")
  ```

- CQL Data Types compatibility:


  | CQL Type                 | Supported |                         Cloud Bigtable Mapping                          |
  | ------------------       | :-------: | :---------------------------------------------------------------------: |
  | text                     |     ✓     |                                RAW BYTES                                |
  | blob                     |     ✓     |                                RAW BYTES                                |
  | timestamp                |     ✓     |                                RAW BYTES                                |
  | int                      |     ✓     |                                RAW BYTES                                |
  | bigint                   |     ✓     |                                RAW BYTES                                |
  | float                    |     ✓     |                                RAW BYTES                                |
  | double                   |     ✓     |                                RAW BYTES                                |
  | boolean                  |     ✓     |                                RAW BYTES                                |
  | map<text, text>          |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, int>           |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, bigint>        |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, float>         |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, double>        |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, boolean>       |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<text, timestamp>     |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, text>     |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, int>      |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, bigint>   |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, float>    |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, double>   |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, boolean>  |     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | map<timestamp, timestamp>|     ✓     |   Col name as col family, MAP key as column qualifier, value as value   |
  | set<text>                |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<int>                 |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<bigint>              |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<float>               |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<double>              |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<boolean>             |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | set<timestamp>           |     ✓     | Col name as col family, SET key as column qualifier, value remain empty |
  | list<text>               |     ✓     | YES                                                                     |
  | list<int>                |     ✓     | YES                                                                     |
  | list<bigint>             |     ✓     | YES                                                                     |
  | list<float>              |     ✓     | YES                                                                     |
  | list<double>             |     ✓     | YES                                                                     |
  | list<boolean>            |     ✓     | YES                                                                     |
  | list<timestamp>          |     ✓     | YES                                                                     |

All list types follow the same storage pattern:
**Col name as col family, current timestamp (with nanosecond precision) as column qualifier, list items as column value.**


- Before running the proxy application, make sure to stop the Cassandra service as both runs on the same port 9042.

## Run a `cassandra-to-bigtable-proxy` via systemd.

Why systemd? - It can automatically restart crashed services, which is crucial for maintaining the availability of applications and system functions. systemd also provides mechanisms for service sandboxing and security.

Go through this document for setting up systemd for this project in your linux system - [Setting Up systemd](./systemd/Readme.md)

## Instrument with OpenTelemetry

The Proxy Adapter supports OpenTelemetry metrics and traces, which gives insight into the adapter internals and aids in debugging/troubleshooting production issues.

See [OpenTelemetry](otel/README.md) for integrating OpenTelemetry (OTEL) with Your Application

[developer-console]: https://console.developers.google.com/
[enable-api]: https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com
[enable-billing]: https://cloud.google.com/apis/docs/getting-started#enabling_billing
[create-project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[cloud-cli]: https://cloud.google.com/cli
[Apache License 2.0](LICENSE)

## Testing

The project includes several types of tests to ensure code quality and functionality. Each test type has its own specific purpose and execution method.

### Unit Tests

Unit tests verify individual components of the codebase in isolation.

```sh
go test -count=1 ./{/proxycore,/proxy,/responsehandler,/schema-mapping,/translator,/collectiondecoder,/bigtable,/utilities,/otel}  -coverprofile=coverage.out
```

### Compliance Tests

Compliance tests ensure that the proxy behaves correctly according to expected standards and specifications.

For detailed instructions on running compliance tests, refer to the [Compliance Test Cases](./testing/compliance/compliance_readme.md) documentation.

### Integration Tests

Integration tests verify that different components of the system work together correctly.

For detailed instructions on running integration tests, refer to the [Integration Tests](./testing/it/README.md) documentation.

### Fuzzing Tests

Fuzzing tests use automated techniques to find edge cases and potential issues in the codebase.

For detailed instructions on running fuzzing tests, refer to the [Fuzzing Tests](./testing/fuzzing/README.md) documentation.

## Connection Methods

The proxy supports three different connection methods to accommodate various deployment scenarios and security requirements.

### Plain TCP Connection

The simplest way to connect to the proxy is using plain TCP. This is suitable for local development or secure internal networks.

**Setup Instructions:**
1. Start the proxy with default TCP settings:
   ```bash
   ./cassandra-to-bigtable-proxy
   ```
   This will listen on `0.0.0.0:9042` by default.

2. Connect using a Cassandra client:
   ```java
   Cluster cluster = Cluster.builder()
       .addContactPoint("localhost")
       .withPort(9042)
       .build();
   Session session = cluster.connect();
   ```

### Secured TCP Connection (TLS)

For production environments or when security is a concern, you can enable TLS encryption.

**Setup Instructions:**
1. If you don't have TLS certificates, generate them:
   - key.pem and cert.pem

  Example:
   ```bash
   openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout key.pem -out cert.pem
   ```
 - Note: Check official documentation for generating certificates

2. Start the proxy with TLS enabled:
   ```bash
   ./cassandra-to-bigtable-proxy --proxy-cert-file cert.pem --proxy-key-file key.pem
   ```

3. Connect using a Cassandra client with TLS:
   ```java
   Cluster cluster = Cluster.builder()
       .addContactPoint("localhost")
       .withPort(9042)
       .withSSL(new SSLOptions())
       .build();
   Session session = cluster.connect();
   ```

### Unix Domain Socket (UDS) Connection

UDS provides better performance and security for local connections. This is ideal for applications running on the same machine as the proxy.

**Setup Instructions:**
1. Install socat (required for bridging TCP to UDS):
   ```bash
   # On macOS
   brew install socat

   # On Ubuntu/Debian
   sudo apt-get install socat
   ```

2. Start the proxy with UDS enabled:
   ```bash
   ./cassandra-to-bigtable-proxy --use-unix-socket
   ```
   This will create a Unix socket at `/tmp/cassandra-proxy.sock` by default.

   You can specify a custom path:
   ```bash
   ./cassandra-to-bigtable-proxy --use-unix-socket --unix-socket-path "/path/to/custom.sock"
   ```

3. Set up socat to bridge TCP to UDS:
   ```bash
   socat TCP-LISTEN:9042,reuseaddr,fork UNIX-CONNECT:/tmp/cassandra-proxy.sock
   ```

4. Connect using a Cassandra client (it will connect via TCP, which socat bridges to UDS):
   ```java
   Cluster cluster = Cluster.builder()
       .addContactPoint("localhost")
       .withPort(9042)
       .build();
   Session session = cluster.connect();
   ```

**Note:** The socat bridge is necessary because the standard Cassandra Java driver doesn't support UDS directly. The bridge allows you to use UDS for better performance while maintaining compatibility with existing clients.

## Supported Cassandra Versions

The Cassandra to Bigtable Proxy has been tested and verified to work with the following Cassandra versions:

- **Cassandra 4.x**: Fully tested and supported
- **Cassandra 3.x**: Should be compatible but not extensively tested

The proxy is configured to use Cassandra 4.0 protocol by default, but it should be able to handle connections from Cassandra 3.x clients. If you're using Cassandra 3.x, please test thoroughly in your environment and report any issues.

To specify a different protocol version when starting the proxy, use the `--protocol-version` flag:

```bash
./cassandra-to-bigtable-proxy --protocol-version v3
```

Note that some features may behave differently between Cassandra 3.x and 4.x.
