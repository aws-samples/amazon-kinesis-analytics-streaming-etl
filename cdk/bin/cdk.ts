#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { StreamingEtl } from '../lib/streaming-etl';

const app = new cdk.App();
new StreamingEtl(app, 'CdkStack');
