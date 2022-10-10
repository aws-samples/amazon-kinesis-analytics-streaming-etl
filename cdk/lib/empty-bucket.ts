import fs = require('fs');
import s3 = require('aws-cdk-lib/aws-s3');
import lambda = require('aws-cdk-lib/aws-lambda');
import cfn = require('aws-cdk-lib/aws-cloudformation');
import { Duration } from 'aws-cdk-lib';
import { Construct } from "constructs";


export interface EmptyBucketOnDeleteProps {
  bucket: s3.Bucket,
}

export class EmptyBucketOnDelete extends Construct {
  customResource: cfn.CfnCustomResource;

  constructor(scope: Construct, id: string, props: EmptyBucketOnDeleteProps) {
    super(scope, id);

    const lambdaSource = fs.readFileSync('lambda/empty-bucket.py').toString();

    const emptyBucketLambda = new lambda.Function(this, 'EmptyBucketLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      timeout: Duration.minutes(15),
      code: lambda.Code.fromInline(lambdaSource),
      handler: 'index.empty_bucket',
      memorySize: 512,
      environment: {
        bucket_name: props.bucket.bucketName,
      }
    });

    props.bucket.grantReadWrite(emptyBucketLambda);

    this.customResource = new cfn.CfnCustomResource(this, 'EmptyBucketResource', {
      serviceToken: emptyBucketLambda.functionArn
    });
  }
}