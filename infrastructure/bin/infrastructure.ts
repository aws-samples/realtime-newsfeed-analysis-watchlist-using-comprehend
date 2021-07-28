#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { RealtimeNewsAnalysisStack } from '../lib/realtime-news-analysis-stack';

const app = new cdk.App();
new RealtimeNewsAnalysisStack(app, 'RealtimeNewsAnalysisStack',
    {
        env: {
            // account: cdk.Aws.ACCOUNT_ID,
            account: process.env.CDK_DEPLOY_ACCOUNT || process.env.CDK_DEFAULT_ACCOUNT || '115272120974',
            region: process.env.CDK_DEPLOY_REGION || process.env.CDK_DEFAULT_REGION || 'us-east-2'
        }
    });
