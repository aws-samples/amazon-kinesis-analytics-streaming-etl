## Kinesis Analytics Streaming ETL Pipeline

A streaming ETL pipeline based on [Apache Flink](https://flink.apache.org/) and [Amazon Kinesis Data Analytics (KDA)](https://aws.amazon.com/kinesis/data-analytics/).

Apache Flink is a framework and distributed processing engine for processing data streams. AWS provides a fully managed service for Apache Flink through Amazon Kinesis Data Analytics, enabling you to quickly build and easily run sophisticated streaming applica-tions with low operational overhead.

<img src="misc/architecture-overview.png?raw=true" width="600" style="display: block; margin-left: auto; margin-right: auto;">

The architecture takes advantage of several capabilities that can be achieved when running Apache Flink with Kinesis Data Analytics. Specifically, the architecture supports:

- **Private Networks Connectivity**: connect to resources in your Amazon Virtual Private Cloud (Amazon VPC), in your data center using a VPN connection, or in a remote region using a VPC peering connection
- **Multiple sources and sinks**: read and write data not only from Amazon Kinesis data streams but also from Apache Kafka or Amazon Managed Streaming for Apache Kafka (Amazon MSK) clusters
- **Data partitioning**: determine the partitioning of data that is ingested into Amazon Simple Storage Service (Amazon S3) based on information extracted from the event payload
- **Multiple Elasticsearch indices and custom document ids**: fan-out from a single input stream to different Elasticsearch indices and explicitly control the document id
- **Exactly-once semantics**: avoid duplicates when ingesting and delivering data between Apache Kafka, Amazon S3, and Amazon Elasticsearch Service (Amazon ES). 

You can find a further details and a more thorough description and discussion of the architecture on the [AWS Big Data Blog]().


## Explore the pipeline in your own AWS account

If you want to explore and play around with the architecture, launch this [AWS CloudFormation template](https://github.com/aws-samples/amazon-kinesis-analytics-streaming-etl/blob/master/cdk/cdk.out/CdkStack.template.json) in your AWS account. The template creates a Kinesis data stream and replays a historic set of set of taxi trips into the data stream. The events are then read by a Kinesis Data Analytics application and persisted to Amazon S3 in Apache Parquet format and partitioned by event time.

To populate the Kinesis data stream, we use a Java application that replays a public dataset of historic taxi trips made in New York City into the data stream. Each event describes a taxi trip made in New York City and includes timestamps for the start and end of a trip, information on the boroughs the trip started and ended in, and various details on the fare of the trip. 

The Java application has already been downloaded to an Amazon Elastic Compute Cloud (Amazon EC2) instance that was provisioned by AWS CloudFormation. You just need to connect to the instance and execute the JAR file to start ingesting events into the stream.

```
$ ssh ec2-user@«Replay instance DNS name»

$ java -jar amazon-kinesis-replay-1.0-SNAPSHOT.jar -noWatermark -objectPrefix artifacts/kinesis-analytics-taxi-consumer/taxi-trips-partitioned.json.lz4/dropoff_year=2018/ -streamName «Kinesis stream name» -speedup 3600
```

You can obtain these commands, including their correct parameters, from the output section of the AWS CloudFormation template that you executed previously.


## Configuration options

With the default configuration, the Kinesis Data Analytics application reads events from a Kinesis data stream and writes them in Apache Parquet format and partitioned by type and event time to Amazon S3. 

The application supports further sources and sinks, in addition to those that are created by the CloudFormation template. You can create additional resources, eg, an Amazon MSK cluster and an Amazon Elasticsearch Service domain, and configure them as additional sources and sinks by adding them to the application's [runtime properties](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-properties.html).

<img src="misc/kda-properties.png?raw=true" width="600" style="display: block; margin-left: auto; margin-right: auto;">

### Configuring additional sources and sinks

You can choose between a single Kinesis data stream and a Kafka topic as input source. In addition you can add a Kinesis data stream, a Kafka topic, an S3 bucket, and Amazon Elasticsearch Service as one or several sinks to the application.

For services that are integrated with AWS Identity and Access Management (IAM), the application uses the role that is configured for the Kinesis Data Analytics application to sign requests. The role created by the CloudFormation template is configured to allow read and write requests to any S3 bucket, Kinesis data stream, and Amazon Elasticsearch Service domain in the AWS account.

#### Amazon Kinesis Data Streams

Amazon Kinesis Data Streams can be used as source and sink. To configure a Kinesis data stream as a source or sink, include these parameters in the application's properties.

- **InputKinesisStream**: the name of the Kinesis data stream to read data from
- **InputStreamRegion** (optional): the region of the Kinesis data stream, defaults to the region of the KDA application
- **OutputKinesisStream**: the name of the Kinesis data stream to write data to
- **OutputStreamRegion** (optional): the region of the Kinesis data stream, defaults to the region of the KDA application

The Kinesis data stream can be in the same or in a different region. You also can use an [Interface VPC Endpoints](#gateway-and-interface-vpc-endpoints) (AWS PrivateLink) to privately connect to your Kinesis data stream.

#### Amazon Managed Streaming for Apache Kafka (Amazon MSK)

Apache Kafka and Amazon MSK clusters can be used as sources and sinks by including the following parameters in the application's properties:

- **InputKafkaBootstrapServers**: comma separated list of broker DNS names and port pairs for the initial connection
- **InputKafkaTopic**: the name of the topic to read data from
- **OutputKafkaBootstrapServers**: comma separated list of broker DNS names and port pairs for the initial connection
- **OutputKafkaTopic**: the name of the topic to read data from

To access Kafka or MSK clusters, you need to configure the Kinesis Data Analytics application to connect your VPC. For details, see [Enabling VPC support](#enabling-vpc-support) below.

#### Amazon Simple Storage Service (Amazon S3)

To write output to Amazon S3 include the following parameters in the application's properties:

- **OutputBucket**: name of the Amazon S3 bucket to persist data to
- **ParquetConversion** optional: whether output should be converted to Apache Parquet format, defaults to `true`

The output on Amazon S3 will be partitioned by boroughs and by the pickup time of the respective events. You can also use a [Gateway VPC Endpoint](#gateway-and-interface-vpc-endpoints) to privately connect to your S3 buckets.

#### Amazon Elasticsearch Service

To write output to Elasticsearch include the following parameters in the application's properties:

- **OutputElasticsearchEndpoint**: the URL to the Elasticsearch endpoint 
- **ElasticsearchRegion** (optional): the region of the Elasticsearch endpoint, defaults to the region of the KDA application

The payload of the event determines the index and ids of documents written to Amazon Elasticsearch Service. More precisely, the event type is mapped to an index (and document type) and the trip id is mapped to the document id.

You can also access Amazon ES domains that are deployed to a VPC by [enabling VPC support](#enabling-vpc-support).

### Enabling VPC support

To connect the Kinesis Data Analytics application to subnets in your VPC you need to enable VPC support.

Kinesis Data Analytics then creates Elastic Network Interfaces (ENIs) in one or more of the subnets provided in your VPC configuration for the application and can access resources that have network connectivity from the configured subnets. This also includes resources that are not directly contained in these subnets but are reachable over a [VPN connection](https://docs.aws.amazon.com/vpc/latest/userguide/vpn-connections.html) or through [VPC peering](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-peering.html).

<img src="misc/kda-vpc-config.png?raw=true" width="600" style="display: block; margin-left: auto; margin-right: auto;">

When you are leveraging VPC peering, eg, to connect to an MSK cluster or Amazon ES domain in a peered VPC, make sure to enable [DNS Resolution Support for the VPC Peering Connection](https://docs.aws.amazon.com/vpc/latest/peering/modify-peering-connections.html#vpc-peering-dns). Otherwise, the Kinesis Data Analytics application is not able to resolve the DNS names of the endpoints in the peered VPC.

When you enable VPC support and need to connect to endpoints that are available over the public internet at the same time, make sure that a [NAT gateway has been configured](https://docs.aws.amazon.com/kinesisanalytics/latest/java/vpc-internet.html) for the respective subnets.

### Gateway and Interface VPC Endpoints

You can also use [Gateway](https://docs.aws.amazon.com/vpc/latest/userguide/vpce-gateway.html) and [Interface](https://docs.aws.amazon.com/vpc/latest/userguide/vpce-interface.html) VPC Endpoints to privately connect to supported AWS services.

In case you are using Interface VPC Endpoints, it's recommended to [enable private DNS](https://docs.aws.amazon.com/vpc/latest/userguide/vpce-interface.html#vpce-private-dns) so that the application can reach the service using its default DNS hostname. 

## License

This library is licensed under the MIT-0 License. See the LICENSE file.