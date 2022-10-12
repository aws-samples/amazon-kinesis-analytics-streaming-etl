import fs = require('fs');
import s3 = require('aws-cdk-lib/aws-s3');
import cfn = require('aws-cdk-lib/aws-cloudformation');
import lambda = require('aws-cdk-lib/aws-lambda');
import codepipeline = require('aws-cdk-lib/aws-codepipeline');
import codepipeline_actions = require('aws-cdk-lib/aws-codepipeline-actions');
import codebuild = require('aws-cdk-lib/aws-codebuild');

import { BuildSpec } from 'aws-cdk-lib/aws-codebuild';
import { Duration } from 'aws-cdk-lib';
import { Repository } from 'aws-cdk-lib/aws-codecommit';
import { Construct } from "constructs";

export interface BuildPipelineProps {
  secondarySourceAction?: codepipeline_actions.S3SourceAction,
  bucket: s3.Bucket,
  extract: boolean,
  objectKey?: string,
}

interface GithupBuildPipelineProps extends BuildPipelineProps {
  github: string,
  buildSpec?: codebuild.BuildSpec,
}

interface CodeCommitBuildPipelineProps extends BuildPipelineProps {
  codecommit: string,
  branch?: string,
  buildSpec: codebuild.BuildSpec
}


export class BuildPipeline extends Construct {
  buildSuccessWaitCondition: cfn.CfnWaitCondition;

  constructor(scope: Construct, id: string, props: GithupBuildPipelineProps | CodeCommitBuildPipelineProps) {
    super(scope, id);

    let sourceAction, buildSpec;

    const sourceOutput = new codepipeline.Artifact();
    const lambdaSource = fs.readFileSync('lambda/build-pipeline-helper.py').toString();

    if ('github' in props) {
      const match = props.github.match(/https:\/\/github.com\/[^\/]+\/([^\/]+)\/archive\/refs\/tags\/([^\/]+)\.zip/);


      if (! match) {
        throw Error(`Expecting valid GitHub archive url, found: ${props.github}`);
      }

      const artifact = match[1];
      const directory = match.slice(1).join('-');
      const key = `sources/${directory}.zip`;

      const downloadLambda =  new lambda.Function(this, 'DownloadLambda', {
        runtime: lambda.Runtime.PYTHON_3_9,
        timeout: Duration.seconds(30),
        code: lambda.Code.fromInline(lambdaSource),
        handler: 'index.download_sources',
        environment: {
          url: props.github,
          bucket: props.bucket.bucketName,
          key: key
        }
      });

      props.bucket.grantPut(downloadLambda);

      new cfn.CfnCustomResource(this, 'DownloadLambdaResource', {
        serviceToken: downloadLambda.functionArn
      });


      sourceAction = new codepipeline_actions.S3SourceAction({
        actionName: 'SourceAction',
        bucket: props.bucket,
        bucketKey: key,
        output: sourceOutput
      });

      if (props.buildSpec) {
        buildSpec = props.buildSpec
      } else {
        buildSpec = BuildSpec.fromObject({
          version: '0.2',
          phases: {
            build: {
              commands: [
                `cd ${directory}`,
                'mvn clean package -B'
              ]
            }
          },
          artifacts: {
            files: [
              `target/${artifact}-*.jar`
            ],
            'discard-paths': false,
            'base-directory': directory
          }
        });
      }

      const cfnId = artifact.split('-').map(s => s.charAt(0).toUpperCase() + s.slice(1)).join('');

      // new cdk.CfnOutput(this, `${cfnId}CopyCommand`, { value: `aws s3 cp --recursive --exclude '*' --include '${artifact}-*.jar' 's3://${props.bucket.bucketName}/target/' .` });
    } else {
      sourceAction = new codepipeline_actions.CodeCommitSourceAction({
        actionName: 'SourceAction',
        repository: Repository.fromRepositoryName(this, 'CodeCommit', props.codecommit),
        branch: props.branch ? props.branch : "master",
        output: sourceOutput
      });

      buildSpec = props.buildSpec
    }


    const project = new codebuild.PipelineProject(this, 'CodebuildProject', {
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_5_0
      },
      buildSpec: buildSpec
    });


    const buildOutput = new codepipeline.Artifact();

    const buildAction = new codepipeline_actions.CodeBuildAction({
      actionName: 'BuildAction',
      project,
      input: sourceOutput,
      extraInputs: props.secondarySourceAction ? props.secondarySourceAction.actionProperties.outputs : undefined,
      outputs: [ buildOutput ]
    });

    const copyAction = new codepipeline_actions.S3DeployAction({
      actionName: 'CopyAction',
      bucket: props.bucket,
      input: buildOutput,
      extract: props.extract,
      objectKey: props.objectKey
    });


    const waitHandle = new cfn.CfnWaitConditionHandle(this, 'WaitHandle');

    this.buildSuccessWaitCondition = new cfn.CfnWaitCondition(this, 'WaitCondition', {
      count: 1,
      handle: waitHandle.ref,
      timeout: Duration.minutes(20).toSeconds().toString()
    });


    const notifyLambda =  new lambda.Function(this, 'NotifyLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromInline(lambdaSource),
      timeout: Duration.seconds(10),
      handler: 'index.notify_build_success',
      environment: {
        waitHandleUrl: waitHandle.ref,
      }
    });

    const notifyAction = new codepipeline_actions.LambdaInvokeAction({
      actionName: 'InvokeAction',
      lambda: notifyLambda,
      runOrder: 2
    });


    new codepipeline.Pipeline(this, 'CodePipeline', {
      stages: [
        {
          stageName: 'Source',
          actions: props.secondarySourceAction ? [sourceAction, props.secondarySourceAction] : [sourceAction]
        },
        {
          stageName: 'Build',
          actions: [buildAction]
        },
        {
          stageName: 'Copy',
          actions: [copyAction, notifyAction]
        }
      ],
      artifactBucket: props.bucket
    });

  }
}