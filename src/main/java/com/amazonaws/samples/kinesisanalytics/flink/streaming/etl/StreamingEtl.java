/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file has been extended from the Apache Flink project skeleton.
 *
 */

package com.amazonaws.samples.kinesisanalytics.flink.streaming.etl;

import com.amazonaws.samples.kinesisanalytics.flink.streaming.etl.events.TripEvent;
import com.amazonaws.samples.kinesisanalytics.flink.streaming.etl.utils.AmazonElasticsearchSink;
import com.amazonaws.samples.kinesisanalytics.flink.streaming.etl.utils.ParameterToolUtils;
import com.amazonaws.samples.kinesisanalytics.flink.streaming.etl.utils.TripEventBucketAssigner;
import com.amazonaws.samples.kinesisanalytics.flink.streaming.etl.utils.TripEventSchema;
import java.io.PrintStream;
import java.util.Properties;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamingEtl {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingEtl.class);

	private static final String DEFAULT_REGION_NAME;

	static {
		String regionName = "eu-west-1";

		try {
			regionName = Regions.getCurrentRegion().getName();
		} catch (Exception e) {
		} finally {
			DEFAULT_REGION_NAME = regionName;
		}
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		DataStream<TripEvent> events;

		if (parameter.has("InputKinesisStream") && (parameter.has("InputKafkaBootstrapServers") || parameter.has("InputKafkaTopic"))) {
			throw new RuntimeException("You can only specify a single source, either a Kinesis data stream or a Kafka topic");
		} else if (parameter.has("InputKinesisStream")) {
			LOG.info("Reading from {} Kinesis stream", parameter.get("InputKinesisStream"));

			events = env
					.addSource(getKinesisSource(parameter))
					.name("Kinesis source");
		} else if (parameter.has("InputKafkaBootstrapServers") && parameter.has("InputKafkaTopic")) {
			LOG.info("Reading from {} Kafka topic", parameter.get("InputKafkaTopic"));

			events = env
					.addSource(getKafkaSource(parameter))
					.name("Kafka source");
		} else {
			throw new RuntimeException("Missing runtime parameters: Specify 'InputKinesisStreamName' xor ('InputKafkaBootstrapServers' and 'InputKafkaTopic') as a parameters to the Flink job");
		}


		if (parameter.has("OutputBucket")) {
			LOG.info("Writing to {} buket", parameter.get("OutputBucket"));

			events
					.keyBy(TripEvent::getPickupLocationId)
					.addSink(getS3Sink(parameter))
					.name("S3 sink");
		}

		if (parameter.has("OutputElasticsearchEndpoint")) {
			LOG.info("Writing to {} ES endpoint", parameter.has("OutputElasticsearchEndpoint"));

			events
					.addSink(getElasticsearchSink(parameter))
					.name("Elasticsearch sink");
		}

		if (parameter.has("OutputKinesisStream")) {
			LOG.info("Writing to {} Kinesis stream", parameter.get("OutputKinesisStream"));

			events
					.addSink(getKinesisSink(parameter))
					.name("Kinesis sink");
		}

		if (parameter.has("OutputDiscarding")) {
			LOG.info("Writing to Discarding sink");

			events
					.addSink(new DiscardingSink<>())
					.name("Discarding sink");
		}

		if (parameter.has("OutputKafkaBootstrapServers") && parameter.has("OutputKafkaTopic")) {
			LOG.info("Writing to {} Kafka topic", parameter.get("OutputKafkaTopic"));

			events
					.addSink(getKafkaSink(parameter))
					.name("Kafka sink");
		}

		if (!(parameter.has("OutputDiscarding") || parameter.has("OutputBucket") || parameter.has("OutputElasticsearchEndpoint") || (parameter.has("OutputKafkaBootstrapServers") && parameter.has("OutputKafkaTopic")) || parameter.has("OutputKinesisStream"))) {
			throw new RuntimeException("Missing runtime parameters: Specify 'OutputDiscarding' or 'OutputBucket' or 'OutputElasticsearchEndpoint' or ('OutputKafkaBootstrapServers' and 'OutputKafkaTopic') as a parameters to the Flink job");
		}

		env.execute();
	}


	private static SourceFunction<TripEvent> getKinesisSource(ParameterTool parameter) {
		String streamName = parameter.getRequired("InputKinesisStream");
		String region = parameter.get("InputStreamRegion", DEFAULT_REGION_NAME);
		String initialPosition = parameter.get("InputStreamInitalPosition", ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION);

		//set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, region);
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
		//poll new events from the Kinesis stream once every second
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");

		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition);

		return new FlinkKinesisConsumer<>(streamName,
				new TripEventSchema(),
				kinesisConsumerConfig
		);
	}


	private static SourceFunction<TripEvent> getKafkaSource(ParameterTool parameter) {
		String topic = parameter.getRequired("InputKafkaTopic");
		String bootstrapServers = parameter.getRequired("InputKafkaBootstrapServers");

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty("group.id", "kaja-streaming-etl-consumer");
		properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		return new FlinkKafkaConsumer<>(topic, new TripEventSchema(), properties);
	}


	private static SinkFunction<TripEvent> getKinesisSink(ParameterTool parameter) {
		String streamName = parameter.getRequired("OutputKinesisStream");
		String region = parameter.get("OutputStreamRegion", DEFAULT_REGION_NAME);

		Properties properties = new Properties();
		properties.setProperty(AWSConfigConstants.AWS_REGION, region);
		properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		FlinkKinesisProducer<TripEvent> producer = new FlinkKinesisProducer<>(new TripEventSchema(), properties);
		producer.setFailOnError(true);
		producer.setDefaultStream(streamName);
		producer.setDefaultPartition("0");

		return producer;
	}

	private static SinkFunction<TripEvent> getKafkaSink(ParameterTool parameter) {
		String topic = parameter.getRequired("OutputKafkaTopic");
		String bootstrapServers = parameter.getRequired("OutputKafkaBootstrapServers");

		return new FlinkKafkaProducer<>(bootstrapServers, topic, new TripEventSchema());
	}


	private static SinkFunction<TripEvent> getS3Sink(ParameterTool parameter) {
		String bucket = parameter.getRequired("OutputBucket");
		String prefix = String.format("%sjob_start=%s/", parameter.get("OutputPrefix", ""), System.currentTimeMillis());

		if (parameter.getBoolean("ParquetConversion", false)) {
			return StreamingFileSink
					.forBulkFormat(
							new Path(bucket),
							ParquetAvroWriters.forSpecificRecord(TripEvent.class)
					)
					.withBucketAssigner(new TripEventBucketAssigner(prefix))
					.build();
		} else {
			return StreamingFileSink
					.forRowFormat(
							new Path(bucket),
							(Encoder<TripEvent>) (element, outputStream) -> {
								PrintStream out = new PrintStream(outputStream);
								out.println(TripEventSchema.toJson(element));
							}
					)
					.withBucketAssigner(new TripEventBucketAssigner(prefix))
					.withRollingPolicy(DefaultRollingPolicy.builder().build())
					.build();
		}
	}

	private static SinkFunction<TripEvent> getElasticsearchSink(ParameterTool parameter) {
		String elasticsearchEndpoint = parameter.getRequired("OutputElasticsearchEndpoint");
		String region = parameter.get("ElasticsearchRegion", DEFAULT_REGION_NAME);

		ElasticsearchSink.Builder<TripEvent> builder =  AmazonElasticsearchSink.elasticsearchSinkBuilder(
				elasticsearchEndpoint,
				region,
				new ElasticsearchSinkFunction<TripEvent>() {
					IndexRequest createIndexRequest(TripEvent element) {
						String type = element.getType().toString();
						String tripId = Long.toString(element.getTripId());

						return Requests.indexRequest()
								.index(type)
								.type(type)
								.id(tripId)
								.source(TripEventSchema.toJson(element), XContentType.JSON);
					}

					@Override
					public void process(TripEvent element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);

		builder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

		if (parameter.has("ElasticsearchBulkFlushMaxSizeMb")) {
			builder.setBulkFlushMaxSizeMb(parameter.getInt("ElasticsearchBulkFlushMaxSizeMb"));
		}

		if (parameter.has("ElasticsearchBulkFlushMaxActions")) {
			builder.setBulkFlushMaxActions(parameter.getInt("ElasticsearchBulkFlushMaxActions"));
		}

		if (parameter.has("ElasticsearchBulkFlushInterval")) {
			builder.setBulkFlushInterval(parameter.getLong("ElasticsearchBulkFlushInterval"));
		}

		return builder.build();
	}
}
