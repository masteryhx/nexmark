# Nexmark Benchmark

## What is Nexmark

Nexmark is a benchmark suite for queries over continuous data streams. This project is inspired by the [NEXMark research paper](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/) and [Apache Beam Nexmark](https://beam.apache.org/documentation/sdks/java/testing/nexmark/).

## Nexmark Benchmark Suite

### Schemas

These are multiple queries over a three entities model representing on online auction system:

- **Person** represents a person submitting an item for auction and/or making a bid on an auction.
- **Auction** represents an item under auction.
- **Bid** represents a bid for an item under auction.

### Queries

| Query  | Name | Summary | Flink |
| -------- | -------- | -------- | ------ |
| q0 | Pass Through | Measures the monitoring overhead including the source generator. | ✅ |
| q1 | Currency Conversion | Convert each bid value from dollars to euros. | ✅ |
| q2 | Selection | Find bids with specific auction ids and show their bid price. | ✅ |
| q3 | Local Item Suggestion | Who is selling in OR, ID or CA in category 10, and for what auction ids?  | ✅ |
| q4 | Average Price for a Category | Select the average of the wining bid prices for all auctions in each category. | ✅ |
| q5 | Hot Items | Which auctions have seen the most bids in the last period? | ✅ |
| q6 | Average Selling Price by Seller | What is the average selling price per seller for their last 10 closed auctions. | [FLINK-19059](https://issues.apache.org/jira/browse/FLINK-19059) |
| q7 | Highest Bid | Select the bids with the highest bid price in the last period. | ✅ |
| q8 | Monitor New Users | Select people who have entered the system and created auctions in the last period. | ✅ |
| q9 | Winning Bids | Find the winning bid for each auction. | ✅ |
| q10 | Log to File System | Log all events to file system. Illustrates windows streaming data into partitioned file system. | ✅ |
| q11 | User Sessions | How many bids did a user make in each session they were active? Illustrates session windows. | ✅ |
| q12 | Processing Time Windows | How many bids does a user make within a fixed processing time limit? Illustrates working in processing time window. | ✅ |
| q13 | Bounded Side Input Join | Joins a stream to a bounded side input, modeling basic stream enrichment. | ✅ |
| q14 | Calculation | Convert bid timestamp into types and find bids with specific price. Illustrates more complex projection and filter.  | ✅ |
| q15 | Bidding Statistics Report | How many distinct users join the bidding for different level of price? Illustrates multiple distinct aggregations with filters. | ✅ |
| q16 | Channel Statistics Report | How many distinct users join the bidding for different level of price for a channel? Illustrates multiple distinct aggregations with filters for multiple keys. | ✅ |
| q17 | Auction Statistics Report | How many bids on an auction made a day and what is the price? Illustrates an unbounded group aggregation. | ✅ |
| q18 | Find last bid | What's a's last bid for bidder to auction? Illustrates a Deduplicate query. | ✅ |
| q19 | Auction TOP-10 Price | What's the top price 10 bids of an auction? Illustrates a TOP-N query. | ✅ |
| q20 | Expand bid with auction | Get bids with the corresponding auction information where category is 10. Illustrates a filter join. | ✅ |
| q21 | Add channel id | Add a channel_id column to the bid table. Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL. | ✅ |
| q22 | Get URL Directories | What is the directory structure of the URL? Illustrates a SPLIT_INDEX SQL. | ✅ |

*Note: q1 ~ q8 are from original [NEXMark queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/), q0 and q9 ~ q13 are from [Apache Beam](https://beam.apache.org/documentation/sdks/java/testing/nexmark), others are extended to cover more scenarios.*

### Metrics

For evaluating the performance, there are two performance measurement terms used in Nexmark that are **cores** and **time**.

Cores is the CPU usage used by the stream processing system. Usually CPU allows preemption, not like memory can be limited. Therefore, how the stream processing system effectively use CPU resources, how much throughput is contributed per core, they are important aspect for streaming performance benchmark.
For Flink, we deploy a CPU usage collector on every worker node and send the usage metric to the benchmark runner for summarizing. We don't use the `Status.JVM.CPU.Load` metric provided by Flink, because it is not accurate.

Time is the cost time for specified number of events executed by the stream processing system. With Cores * Time, we can know how many resources the stream processing system uses to process specified number of events.

## Nexmark Benchmark Guideline

### Requirements

The Nexmark benchmark framework runs Flink queries on [standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.13/ops/deployment/cluster_setup.html), see the Flink documentation for more detailed requirements and how to setup it.

#### Software Requirements

The cluster should consist of **one master node** and **one or more worker nodes**. All of them should be **Linux** environment (the CPU monitor script requries to run on Linux). Please make sure you have the following software installed **on each node**:

- **JDK 1.8.x** or higher (Nexmark scripts uses some tools of JDK),
- **ssh** (sshd must be running to use the Flink and Nexmark scripts that manage
  remote components)

If your cluster does not fulfill these software requirements you will need to install/upgrade it.

Having [**passwordless SSH**](https://linuxize.com/post/how-to-setup-passwordless-ssh-login/) and __the same directory structure__ on all your cluster nodes will allow you to use our scripts to control
everything.

#### Environment Variables

The following environment variable should be set on every node for the Flink and Nexmark scripts, JDK8 is used by default.

 - `JAVA_HOME`: point to the directory of your JDK installation.
 - `FLINK_HOME`: point to the directory of your Flink installation.

### Build Nexmark

Before start to run the benchmark, you should build the Nexmark benchmark first to have a benchmark package. Please make sure you have installed `maven` in your build machine. And run the `./build.sh` command under `nexmark-flink` directoy. Then you will get the `nexmark-flink.tgz` archive under the directory.

### Setup Cluster

- Step1: Download the Flink package of 1.16.2 from the [download page](https://flink.apache.org/downloads.html). Say `flink-<version>-bin-scala_2.11.tgz`.
- Step2: Copy the archives (`flink-<version>-bin-scala_2.11.tgz`, `nexmark-flink.tgz`) to your master node and extract it.
  ```
  tar xzf flink-<version>-bin-scala_2.11.tgz; tar xzf nexmark-flink.tgz
  mv flink-<version> flink; mv nexmark-flink nexmark
  ```
- Step3: Copy the jars under `nexmark/lib` to `flink/lib` which contains the Nexmark source generator.
- Step4: Configure Flink.
  - Edit `flink/conf/workers` and enter the IP address of each worker node. Recommand to set 8 entries.
  - Replace `flink/conf/flink-conf.yaml` by `nexmark/conf/flink-conf.yaml`. Remember to update the following configurations:
    - Set `jobmanager.rpc.address` to you master IP address
    - Set `state.checkpoints.dir` to your s3 path, e.g. `s3://benchmark/flink/checkpoint`.
    - Set `s3.access-key` to your access key and set `s3.secret-key` to your secret key. 
    - Set `state.backend.rocksdb.localdir` to your local file path (use SSD), e.g. `/home/username/rocksdb`.
  - Download the s3 plugin jar from [download page](https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.16.2/flink-s3-fs-hadoop-1.16.2.jar) to flink/lib/
- Step5: Configure Nexmark benchmark.
  - Edit `nexmark/conf/nexmark.yaml` and set `nexmark.metric.reporter.host` to your master IP address.
- Step6: Copy `flink` and `nexmark` to your worker nodes using `scp`.
- Step7: Start Flink Cluster by running `flink/bin/start-cluster.sh` on the master node.
- Step8: Setup the benchmark cluster by running `nexmark/bin/setup_cluster.sh` on the master node.
- (If you want to use kafka source instead of datagen source) Step9: Prepare Kafka
  - (Please make sure Flink Kafka Jar is ready in flink/lib/ [download page](https://ci.apache.org/projects/flink/flink-docs-release-1.16/docs/connectors/table/kafka/#dependencies))
  - Start your kafka cluster. (recommend to use SSD)
  - Create kafka topic: `bin/kafka-topics.sh --create --topic nexmark --bootstrap-server localhost:9092 --partitions 24`.
  - Edit `nexmark/conf/nexmark.yaml`, set `kafka.bootstrap.servers`.
  - Prepare source data: `nexmark/bin/run_query.sh insert_kafka`.
  - NOTE: Kafka source is endless, only supports tps mode (unlimited events.num) now.

### Run Nexmark

You can run the Nexmark benchmark by running `nexmark/bin/run_query.sh all` on the master node. It will run all the queries one by one, and collect benchmark metrics automatically. At last, it will print the benchmark summary result (Average Throughput for each query) on the console.

You can also run specific queries by running `nexmark/bin/run_query.sh q1,q2`.

You can also tune the workload of the queries by editing `nexmark/conf/nexmark.yaml` with the `nexmark.workload.*` prefix options.

## Nexmark Benchmark Result

### Machines

Minimum requirements:
- 3 worker node 
- Each machine has 8 cores and 32 GB RAM (AWS EC2 m6id.2xlarge)
- 500 GB SSD local disk (NVMe)

### Flink Configuration

Use the default configuration file `flink-conf.yaml` defined in `nexmark-flink/src/main/resources/conf/`.

Some notable configurations including:

- 6 TaskManagers, each has 4 slot
- 16 GB for each TaskManager and JobManager
- Job parallelism: 24
- Checkpoint enabled with exactly once mode and 3 minutes interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 seconds interval and 50000 rows
- Splitting distinct aggregation optimization is enabled
- Pipeline object reuse is enabled

Flink version: 1.16.2.

### Workloads

Deploy Kafka on a standalone machine (AWS EC2 m6id.2xlarge), using docker is recommended. Running `nexmark/bin/run_query.sh insert_kafka`, Use Nexmark builtin datagen generate 1 billion rows write to Kafka. All queries use Kafka as the data source.

Use the kafka source, some notable configurations including:

- Kafka is unbounded source, only support tps mode, please set `nexmark.workload.suite.1000m.events.num` to 0 in `nexmark/conf/nexmark.yaml`
- Please set `nexmark.metric.monitor.duration` to 30 minutes in `nexmark/conf/nexmark.yaml`. If the job can't be finished within 30 minutes, we only take the first 30 minutes metrics


### Benchmark Results
This is the latest result using Kafka as data source.

```
+-------------------+---------------------+----------------------------------+
| Nexmark Query     | Events Num          | Average Throughput(rows/second)  |
+-------------------+---------------------+----------------------------------+
|q0                 |100,000,000,0        |1289k                             |
|q1                 |100,000,000,0        |1260k                             |
|q2                 |100,000,000,0        |1250k                             |
|q3                 |100,000,000,0        |1270k                             |
|q4                 |100,000,000,0        |464k                              |
|q5                 |100,000,000,0        |653k                              |
|q5_rewrite         |100,000,000,0        |906k                              |
|q7                 |100,000,000,0        |447k                              |
|q7_rewrite         |100,000,000,0        |1330k                             |
|q8                 |100,000,000,0        |1305k                             |
|q9                 |100,000,000,0        |252k                              |
|q10                |100,000,000,0        |1270k                             |
|q11                |100,000,000,0        |582k                              |
|q12                |100,000,000,0        |1260k                             |
|q13                |100,000,000,0        |1340k                             |
|q14                |100,000,000,0        |1340k                             |
|q15                |100,000,000,0        |1230k                             |
|q16                |100,000,000,0        |333k                              |
|q17                |100,000,000,0        |1170k                             |
|q18                |100,000,000,0        |478k                              |
|q19                |100,000,000,0        |362k                              |
|q20                |100,000,000,0        |328k                              |
|q21                |100,000,000,0        |1280k                             |
|q22                |100,000,000,0        |1260k                             |
|Total              |2,400,000,000,0      |22668k                            |
+-------------------+---------------------+----------------------------------+
```

## Roadmap

1. Run Nexmark benchmark for more stream processing systems, such as Spark, KSQL. However, they don't have complete streaming SQL features. Therefore, not all of the queries can be ran in these systems. But we can implement the queries in programing way using Spark Streaming, Kafka Streams.
2. Support Latency metric for the benchmark. Latency measures the required time from a record entering the system to some results produced after some actions performed on the record. However, this is not easy to support for SQL queries unless we modify the queries.

## References

- Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier. NEXMark – A Benchmark for Queries over Data Streams. June 2010.

