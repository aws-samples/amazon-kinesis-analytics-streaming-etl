import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import ec2 = require('@aws-cdk/aws-ec2');
import iam = require('@aws-cdk/aws-iam');
import logs = require('@aws-cdk/aws-logs');
import kds = require('@aws-cdk/aws-kinesis');
import kda = require('@aws-cdk/aws-kinesisanalytics');
import cloudwatch = require('@aws-cdk/aws-cloudwatch');

import { Metric } from '@aws-cdk/aws-cloudwatch';
import { RemovalPolicy, Duration } from '@aws-cdk/core';
import { RetentionDays } from '@aws-cdk/aws-logs';
import { InstanceType, InstanceClass, InstanceSize, AmazonLinuxImage, UserData, AmazonLinuxGeneration } from '@aws-cdk/aws-ec2';
import { BuildArtifacts } from './build-artifacts';
import { EmptyBucketOnDelete } from './empty-bucket';


export class StreamingEtl extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.templateOptions.description = 'Creates a sample streaming ETL pipeline based on Apache Flink and Amazon Kinesis Data Analytics that reads data from a Kinesis data stream and persists it to Amazon S3 (shausma-kda-streaming-etl)';

    const bucket = new s3.Bucket(this, 'Bucket', {
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      metrics: [{
        id: 'EntireBucket',
      }],
      lifecycleRules: [{
        abortIncompleteMultipartUploadAfter: Duration.days(7)
      }]
    });

    const emptyBucket = new EmptyBucketOnDelete(this, 'EmptyBucket', {
      bucket: bucket
    });

    new cdk.CfnOutput(this, `OutputBucket`, { value: `https://console.aws.amazon.com/s3/buckets/${bucket.bucketName}/streaming-etl-output/` });



    const artifacts = new BuildArtifacts(this, 'BuildArtifacts', {
      bucket: bucket,
      flinkConsumerVersion: '0.1.1',
      kinesisReplayVersion: '0.1.1'
    });


    const stream = new kds.Stream(this, 'InputStream', {
      shardCount: 16
    });


    const logGroup = new logs.LogGroup(this, 'KdaLogGroup', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY
    });

    const logStream = new logs.LogStream(this, 'KdaLogStream', {
      logGroup: logGroup,
      removalPolicy: RemovalPolicy.DESTROY
    });

    const logStreamArn = `arn:${cdk.Aws.PARTITION}:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`;

    const kdaRole = new iam.Role(this, 'KdaRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
    });

    bucket.grantReadWrite(kdaRole);
    stream.grantRead(kdaRole);

    kdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'kinesis:ListShards' ],
      resources: [ stream.streamArn ]
    }))

    kdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'cloudwatch:PutMetricData' ],
      resources: [ '*' ]
    }));

    kdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'logs:DescribeLogStreams', 'logs:DescribeLogGroups' ],
      resources: [
        logGroup.logGroupArn,
        `arn:${cdk.Aws.PARTITION}:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:*`
      ]
    }));

    kdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'logs:PutLogEvents' ],
      resources: [ logStreamArn ]
    }));


    const kdaApp = new kda.CfnApplicationV2(this, 'KdaApplication', {
      runtimeEnvironment: 'FLINK-1_8',
      serviceExecutionRole: kdaRole.roleArn,
      applicationName: `${cdk.Aws.STACK_NAME}`,
      applicationConfiguration: {
        environmentProperties: {
          propertyGroups: [
            {
              propertyGroupId: 'FlinkApplicationProperties',
              propertyMap: {
                OutputBucket: `s3://${bucket.bucketName}/streaming-etl-output/`,
                ParquetConversion: true,
                InputKinesisStream: stream.streamName
              },
            }
          ]
        },
        flinkApplicationConfiguration: {
          monitoringConfiguration: {
            logLevel: 'INFO',
            metricsLevel: 'TASK',
            configurationType: 'CUSTOM'
          },
          parallelismConfiguration: {
            autoScalingEnabled: false,
            parallelism: 2,
            parallelismPerKpu: 1,
            configurationType: 'CUSTOM'
          },
          checkpointConfiguration: {
            configurationType: "CUSTOM",
            checkpointInterval: 60_000,
            minPauseBetweenCheckpoints: 60_000,
            checkpointingEnabled: true
          }
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: false
        },
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: bucket.bucketArn,
              fileKey: 'target/amazon-kinesis-analytics-streaming-etl-1.0-SNAPSHOT.jar'        
            }
          },
          codeContentType: 'ZIPFILE'
        }
      }
    });

    new kda.CfnApplicationCloudWatchLoggingOptionV2(this, 'KdsFlinkProducerLogging', {
        applicationName: kdaApp.ref.toString(),
        cloudWatchLoggingOption: {
          logStreamArn: logStreamArn
        }
    });

    kdaApp.addDependsOn(artifacts.consumerBuildSuccessWaitCondition);
    kdaApp.addDependsOn(emptyBucket.customResource);       //ensures that the app is stopped before the bucket is emptied


    const vpc = new ec2.Vpc(this, 'VPC', {
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
        }
      ]
    });
    
    const producerRole = new iam.Role(this, 'ReplayRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')
      ]
    });

    stream.grantReadWrite(producerRole);
    producerRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'kinesis:ListShards' ],
      resources: [ stream.streamArn ]
    }));

    bucket.grantRead(producerRole);
    s3.Bucket.fromBucketName(this, 'BigDataBucket', 'aws-bigdata-blog').grantRead(producerRole);

    producerRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'cloudwatch:PutMetricData' ],
      resources: [ '*' ]
    }));

    producerRole.addToPolicy(new iam.PolicyStatement({
      actions: [ 'kinesisanalytics:StartApplication', 'kinesisanalytics:StopApplication', 'kinesisanalytics:DescribeApplication', 'kinesisanalytics:UpdateApplication' ],
      resources: [ `arn:${cdk.Aws.PARTITION}:kinesisanalytics:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:application/${kdaApp.applicationName}` ]
    }));

    const userData = UserData.forLinux()

    const instance = new ec2.Instance(this, 'ProducerInstance', {
      vpc: vpc,
      vpcSubnets: {
        subnets: vpc.publicSubnets
      },
      instanceType: InstanceType.of(InstanceClass.C5N, InstanceSize.LARGE),
      machineImage: new AmazonLinuxImage({
        generation: AmazonLinuxGeneration.AMAZON_LINUX_2
      }),
      instanceName: `${cdk.Aws.STACK_NAME}/ProducerInstance`,
      userData: userData,
      role: producerRole,
      resourceSignalTimeout: Duration.minutes(5)
    }).instance;

    userData.addCommands(
      'yum install -y tmux jq java-11-amazon-corretto-headless',
      `aws s3 cp --recursive --no-progress --exclude '*' --include 'amazon-kinesis-replay-*.jar' 's3://${bucket.bucketName}/target/' /tmp`,
      `aws --region ${cdk.Aws.REGION} kinesisanalyticsv2 start-application --application-name ${kdaApp.ref} --run-configuration '{ "ApplicationRestoreConfiguration": { "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT" } }'`,
      `/opt/aws/bin/cfn-signal -e $? --stack ${cdk.Aws.STACK_NAME} --resource ${instance.logicalId} --region ${cdk.Aws.REGION}`
    );
    
    instance.addDependsOn(kdaApp);

    new cdk.CfnOutput(this, 'ReplayCommand', { value: `java -jar /tmp/amazon-kinesis-replay-1.0-SNAPSHOT.jar -streamName ${stream.streamName} -noWatermark -objectPrefix artifacts/kinesis-analytics-taxi-consumer/taxi-trips-partitioned.json.lz4/dropoff_year=2018/ -speedup 3600` });
    new cdk.CfnOutput(this, 'ConnectToInstance', { value: `https://console.aws.amazon.com/systems-manager/session-manager/${instance.ref}`});


    const dashboard = new cloudwatch.Dashboard(this, 'Dashboard', {
      dashboardName: cdk.Aws.STACK_NAME
    });

    const incomingRecords = new Metric({
      namespace: 'AWS/Kinesis',
      metricName: 'IncomingRecords',
      dimensions: {
        StreamName: stream.streamName
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });

    const incomingBytes = new Metric({
      namespace: 'AWS/Kinesis',
      metricName: 'IncomingBytes',
      dimensions: {
        StreamName: stream.streamName
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });

    const outgoingRecords = new Metric({
      namespace: 'AWS/Kinesis',
      metricName: 'GetRecords.Records',
      dimensions: {
        StreamName: stream.streamName
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });

    const outgoingBytes = new Metric({
      namespace: 'AWS/Kinesis',
      metricName: 'GetRecords.Bytes',
      dimensions: {
        StreamName: stream.streamName
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });

    const millisBehindLatest = new Metric({
      namespace: 'AWS/KinesisAnalytics',
      metricName: 'millisBehindLatest',
      dimensions: {
        Id: cdk.Fn.join('_', cdk.Fn.split('-', stream.streamName)),
        Application: kdaApp.ref,
        Flow: 'Input'
      },
      period: Duration.minutes(1),
      statistic: 'max',
    });

    const bytesUploaded = new Metric({
      namespace: 'AWS/S3',
      metricName: 'BytesUploaded',
      dimensions: {
        BucketName: bucket.bucketName,
        FilterId: 'EntireBucket'
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });

    const putRequests = new Metric({
      namespace: 'AWS/S3',
      metricName: 'PutRequests',
      dimensions: {
        BucketName: bucket.bucketName,
        FilterId: 'EntireBucket'
      },
      period: Duration.minutes(1),
      statistic: 'sum'
    });
    

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [incomingRecords],
        right: [incomingBytes],
        width: 24,
        title: 'Kinesis data stream (incoming)',
        leftYAxis: {
          min: 0
        },
        rightYAxis: {
          min: 0
        }
      })
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [outgoingRecords],
        right: [outgoingBytes],
        width: 24,
        title: 'Kinesis data stream (outgoing)',
        leftYAxis: {
          min: 0
        },
        rightYAxis: {
          min: 0
        }
      })
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [
          millisBehindLatest,
          millisBehindLatest.with({
            statistic: "avg"
          })
        ],
        width: 24,
        title: 'Flink consumer lag',
        leftYAxis: {
          label: 'milliseconds',
          showUnits: false,
          min: 0
        }
      })
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [putRequests],
        right: [bytesUploaded],
        width: 24,
        title: 'Amazon S3 (incoming)',
        leftYAxis: {
          min: 0
        },
        rightYAxis: {
          min: 0
        }
      })
    );

    new cdk.CfnOutput(this, 'CloudwatchDashboard', { value: `https://console.aws.amazon.com/cloudwatch/home#dashboards:name=${cdk.Aws.STACK_NAME}` });
    new cdk.CfnOutput(this, 'CloudwatchLogsInsights', { value: `https://console.aws.amazon.com/cloudwatch/home#logs-insights:queryDetail=~(end~0~source~'${logGroup.logGroupName}~start~-3600~timeType~'RELATIVE~unit~'seconds)` });
  }
}