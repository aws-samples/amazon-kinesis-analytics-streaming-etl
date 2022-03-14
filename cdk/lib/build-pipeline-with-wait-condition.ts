import fs = require('fs');
import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import cfn = require('@aws-cdk/aws-cloudformation');
import lambda = require('@aws-cdk/aws-lambda');
import codepipeline = require('@aws-cdk/aws-codepipeline');
import codepipeline_actions = require('@aws-cdk/aws-codepipeline-actions');
import codebuild = require('@aws-cdk/aws-codebuild');
import { BuildSpec } from '@aws-cdk/aws-codebuild';
import { Duration } from '@aws-cdk/core';
import { CustomResourceProvider } from '@aws-cdk/aws-cloudformation';
import { Repository } from '@aws-cdk/aws-codecommit';

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


export class BuildPipeline extends cdk.Construct {
  buildSuccessWaitCondition: cfn.CfnWaitCondition;

  constructor(scope: cdk.Construct, id: string, props: GithupBuildPipelineProps | CodeCommitBuildPipelineProps) {
    super(scope, id);

    var sourceAction, buildSpec;

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
        runtime: lambda.Runtime.PYTHON_3_7,
        timeout: Duration.seconds(30),
        code: lambda.Code.inline(lambdaSource),
        handler: 'index.download_sources',
        environment: {
          url: props.github,
          bucket: props.bucket.bucketName,
          key: key
        }
      });

      props.bucket.grantPut(downloadLambda);

      new cfn.CustomResource(this, 'DownloadLambdaResource', {
        provider: CustomResourceProvider.lambda(downloadLambda)
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
        buildImage: codebuild.LinuxBuildImage.UBUNTU_14_04_OPEN_JDK_11
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
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.inline(lambdaSource),
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