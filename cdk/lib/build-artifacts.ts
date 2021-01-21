import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import cfn = require('@aws-cdk/aws-cloudformation');
import codepipeline = require('@aws-cdk/aws-codepipeline');
import codepipeline_actions = require('@aws-cdk/aws-codepipeline-actions');
import { BuildSpec } from '@aws-cdk/aws-codebuild';
import { BuildPipeline } from './build-pipeline-with-wait-condition';

export interface BuildArtifactsProps {
  bucket: s3.Bucket,
  flinkConsumerVersion: string
  kinesisReplayVersion: string
}

export class BuildArtifacts extends cdk.Construct {
    consumerBuildSuccessWaitCondition: cfn.CfnWaitCondition;
    producerBuildSuccessWaitCondition: cfn.CfnWaitCondition;
  
    constructor(scope: cdk.Construct, id: string, props: BuildArtifactsProps) {
        super(scope, id);

        const producer = new BuildPipeline(this, 'KinesisReplayBuildPipeline', {
            bucket: props.bucket,
            github: `https://github.com/aws-samples/amazon-kinesis-replay/archive/${props.kinesisReplayVersion}.zip`,
            extract: true
        });

        this.producerBuildSuccessWaitCondition = producer.buildSuccessWaitCondition;
    
        const consumer = new BuildPipeline(this, 'FlinkConsumer', {
          bucket: props.bucket,
          github: `https://github.com/aws-samples/amazon-kinesis-analytics-streaming-etl/archive/${props.flinkConsumerVersion}.zip`, 
          extract: true,
        });

        this.consumerBuildSuccessWaitCondition = consumer.buildSuccessWaitCondition;
    }
}
