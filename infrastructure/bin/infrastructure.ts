#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { LinxIngressStack } from '../lib/linx-ingress-stack';
import { LinxProcessingStack } from '../lib/linx-processing-stack';

const app = new cdk.App();

const env = {
  // Target account and region for all Linx ingress resources.
  account: '093711202389',
  region: 'eu-west-2',
};

new LinxIngressStack(app, 'LinxIngressStack', {
  description: 'Linx data ingress, store and retrieval — Kinesis data streams',
  env,
});

new LinxProcessingStack(app, 'LinxProcessingStack', {
  description: 'Linx data store & retrieval — DynamoDB, consumer Lambdas, query API',
  env,
});

app.synth();
