import os
import json
import boto3
import traceback
import cfnresponse
import urllib.request

s3client = boto3.client('s3')
code_pipeline = boto3.client('codepipeline')


def download_sources(event, context):
    url = os.environ['url']
    bucket = os.environ['bucket']
    key = os.environ['key']

    try:
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)

        s3client.put_object(Bucket=bucket, Key=key, Body=response.read())

        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
    except Exception:
        traceback.print_exc()

        cfnresponse.send(event, context, cfnresponse.FAILED, {})


def notify_build_success(event, context):
    job_id = event['CodePipeline.job']['id']

    url = os.environ['waitHandleUrl']
    headers = { "Content-Type": "" }
    data = { "Status": "SUCCESS", "Reason": "Compilation Succeeded", "UniqueId": job_id, "Data": "Compilation Succeeded" }

    try:
        req = urllib.request.Request(url, headers=headers, data=bytes(json.dumps(data), encoding="utf-8"), method='PUT')
        response = urllib.request.urlopen(req)

        code_pipeline.put_job_success_result(jobId=job_id)
    except Exception:
        traceback.print_exc()

        code_pipeline.put_job_failure_result(jobId=job_id, failureDetails={'type': 'JobFailed'})
