import s3 = require('aws-cdk-lib/aws-s3');
import cfn = require('aws-cdk-lib/aws-cloudformation');
import { BuildPipeline } from './build-pipeline-with-wait-condition';
import { Construct} from "constructs";

export interface BuildArtifactsProps {
  bucket: s3.Bucket,
  flinkConsumerVersion: string
  kinesisReplayVersion: string
}

export class BuildArtifacts extends Construct {
    consumerBuildSuccessWaitCondition: cfn.CfnWaitCondition;
    producerBuildSuccessWaitCondition: cfn.CfnWaitCondition;
  
    constructor(scope: Construct, id: string, props: BuildArtifactsProps) {
        super(scope, id);

        const producer = new BuildPipeline(this, 'KinesisReplayBuildPipeline', {
            bucket: props.bucket,
            github: `https://github.com/aws-samples/amazon-kinesis-replay/archive/refs/tags/${props.kinesisReplayVersion}.zip`,
            extract: true
        });

        this.producerBuildSuccessWaitCondition = producer.buildSuccessWaitCondition;
    
        const consumer = new BuildPipeline(this, 'FlinkConsumer', {
          bucket: props.bucket,
          github: `https://github.com/aws-samples/amazon-kinesis-analytics-streaming-etl/archive/refs/tags/${props.flinkConsumerVersion}.zip`, 
          extract: true,
        });

        this.consumerBuildSuccessWaitCondition = consumer.buildSuccessWaitCondition;
    }
}
