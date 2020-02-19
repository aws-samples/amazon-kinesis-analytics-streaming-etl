import fs = require('fs');
import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import lambda = require('@aws-cdk/aws-lambda');
import cfn = require('@aws-cdk/aws-cloudformation');
import { Duration } from '@aws-cdk/core';
import { CustomResourceProvider } from '@aws-cdk/aws-cloudformation';


export interface EmptyBucketOnDeleteProps {
    bucket: s3.Bucket,
}

export class EmptyBucketOnDelete extends cdk.Construct {
    constructor(scope: cdk.Construct, id: string, props: EmptyBucketOnDeleteProps) {
        super(scope, id);

        const lambdaSource = fs.readFileSync('lambda/empty-bucket.py').toString();

        const emptyBucketLambda =  new lambda.Function(this, 'EmptyBucketLambda', {
            runtime: lambda.Runtime.PYTHON_3_7,
            timeout: Duration.minutes(15),
            code: lambda.Code.inline(lambdaSource),
            handler: 'index.empty_bucket',
            memorySize: 512,
            environment: {
                bucket_name: props.bucket.bucketName,
            }
        });

        props.bucket.grantReadWrite(emptyBucketLambda);

        new cfn.CustomResource(this, 'EmptyBucketResource', {
            provider: CustomResourceProvider.lambda(emptyBucketLambda)
        });
    }
}