#!/usr/bin/env node
import 'source-map-support/register';
import { App } from "aws-cdk-lib";
import { StreamingEtl } from '../lib/streaming-etl';

const app = new App();
new StreamingEtl(app, 'StreamingEtl');
