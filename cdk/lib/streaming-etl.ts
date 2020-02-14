import * as cdk from '@aws-cdk/core';
import s3 = require('@aws-cdk/aws-s3');
import ec2 = require('@aws-cdk/aws-ec2');
import iam = require('@aws-cdk/aws-iam');
import logs = require('@aws-cdk/aws-logs');
import kds = require('@aws-cdk/aws-kinesis');
import kda = require('@aws-cdk/aws-kinesisanalytics');
import cloudwatch = require('@aws-cdk/aws-cloudwatch')

import { Metric } from '@aws-cdk/aws-cloudwatch';
import { RemovalPolicy, Duration } from '@aws-cdk/core';
import { RetentionDays } from '@aws-cdk/aws-logs';
import { InstanceType, InstanceClass, InstanceSize, AmazonLinuxImage, UserData, AmazonLinuxGeneration } from '@aws-cdk/aws-ec2';
import { BuildArtifacts } from './build-artifacts';
import { EmptyBucketOnDelete } from './empty-bucket';


export class StreamingEtl extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.templateOptions.description = 'Creates a sample streaming ETL pipeline based on Apache Flink and Amazon Kinesis Data Analytics that reads data from a Kinesis data stream and persists it to Amazon S3 (shausma-streaming-etl)';

    const bucket = new s3.Bucket(this, 'Bucket', {
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY
    });

    new EmptyBucketOnDelete(this, 'EmptyBucket', {
      bucket: bucket
    });

    new cdk.CfnOutput(this, `OutputBucket`, { value: `https://console.aws.amazon.com/s3/buckets/${bucket.bucketName}/streaming-etl-output/` });



    const artifacts = new BuildArtifacts(this, 'BuildArtifacts', {
      bucket: bucket
    });


    const stream = new kds.Stream(this, 'InputStream', {
      shardCount: 16
    });


    const kdaRole = new iam.Role(this, 'KdaRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
    });

    kdaRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'cloudwatch:PutMetricData',
        'logs:PutLogEvents', 'logs:DescribeLogGroups', 'logs:DescribeLogStreams',
        'kinesis:DescribeStream', 'kinesis:ListShards', 'kinesis:GetShardIterator', 'kinesis:GetRecords',
        'kinesis:PutRecord', 'kinesis:PutRecords',
        's3:GetObject', 's3:List*', 's3:PutObject', 's3:AbortMultipartUpload',
        'es:ESHttpPut', 'es:ESHttpPost', 'es:ESHttpHead',
      ],
      resources: ['*']
    }));

    const logGroup = new logs.LogGroup(this, 'KdaLogGroup', {
      retention: RetentionDays.ONE_WEEK
    });

    const logStream = new logs.LogStream(this, 'KdaLogStream', {
      logGroup: logGroup
    });

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
              parallelism: 3,
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
          logStreamArn: `arn:${cdk.Aws.PARTITION}:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`
        }
    });

    kdaApp.addDependsOn(artifacts.consumerBuildSuccessWaitCondition);


    const sshKeyName = new cdk.CfnParameter(this, 'SshKeyName', {
      type: 'AWS::EC2::KeyPair::KeyName'
    }).valueAsString;

    const vpc = new ec2.Vpc(this, 'VPC', {
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
        }
      ]
    });

    const sg = new ec2.SecurityGroup(this, 'SecurityGroup', {
      vpc: vpc
    });

    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22));
    sg.addIngressRule(sg, ec2.Port.allTraffic());
    
    const role = new iam.Role(this, 'ReplayRole', {
        assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com')
    });

    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'cloudwatch:PutMetricData',
        'kinesis:DescribeStream', 'kinesis:ListShards', 'kinesis:GetShardIterator', 'kinesis:GetRecords',
        'kinesis:PutRecord', 'kinesis:PutRecords',
        'kinesisanalytics:StartApplication',
        's3:GetObject', 's3:List*',
      ],
      resources: ['*']
    }));
    

    const replayCopyCommand = `aws s3 cp --recursive --no-progress --exclude '*' --include 'amazon-kinesis-replay-*.jar' 's3://${bucket.bucketName}/target/' .`

    const userData = UserData.forLinux()
    userData.addCommands(
      'yum install -y tmux jq java-11-amazon-corretto-headless',
      `aws --region ${cdk.Aws.REGION} kinesisanalyticsv2 start-application --application-name ${kdaApp.ref} --run-configuration '{ "ApplicationRestoreConfiguration": { "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT" } }'`,
      `su ec2-user -l -c "${replayCopyCommand}"`
    );

    const instance = new ec2.Instance(this, 'ProducerInstance', {
      vpc: vpc,
      vpcSubnets: {
        subnets: vpc.publicSubnets
      },
      keyName: sshKeyName,
      instanceType: InstanceType.of(InstanceClass.C5N, InstanceSize.XLARGE),
      machineImage: new AmazonLinuxImage({
        generation: AmazonLinuxGeneration.AMAZON_LINUX_2
      }),
      securityGroup: sg,
      userData: userData,
      role: role,
      resourceSignalTimeout: Duration.minutes(5)
    });

    
    userData.addCommands(`/opt/aws/bin/cfn-signal -e $? --stack ${cdk.Aws.STACK_NAME} --resource ${instance.instance.logicalId} --region ${cdk.Aws.REGION}`);
    instance.node.addDependency(artifacts.producerBuildSuccessWaitCondition);

    new cdk.CfnOutput(this, `ProducerInstanceSsh`, { value: `ssh ec2-user@${instance.instancePublicDnsName}` });
    new cdk.CfnOutput(this, `ReplayCommand`, { value: `java -jar amazon-kinesis-replay-1.0-SNAPSHOT.jar -streamName ${stream.streamName} -noWatermark -objectPrefix artifacts/kinesis-analytics-taxi-consumer/taxi-trips-partitioned.json.lz4/dropoff_year=2018/ -speedup 3600` });


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
      statistic: 'max'
    });


    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [incomingRecords],
        right: [incomingBytes],
        width: 24
      })
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [outgoingRecords],
        right: [outgoingBytes],
        width: 24
      })
    );

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        left: [millisBehindLatest],
        width: 24
      })
    );

    new cdk.CfnOutput(this, 'CloudwatchDashboard', { value: `https://console.aws.amazon.com/cloudwatch/home#dashboards:name=${cdk.Aws.STACK_NAME}` });
    new cdk.CfnOutput(this, 'CloudwatchLogsInsights', { value: `https://console.aws.amazon.com/cloudwatch/home#logs-insights:queryDetail=~(end~0~source~'${logGroup.logGroupName}~start~-3600~timeType~'RELATIVE~unit~'seconds)` });
  }
}