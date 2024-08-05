#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { MyCopierLambdaStack } from '../lib/my_copier_lambda_satck';
import { MyCleanerLambdaStack } from '../lib/my_cleaner_lambda_stack';

const app = new cdk.App();
new MyCopierLambdaStack(app, 'MyCopierLambdaStack', {
});

new MyCleanerLambdaStack(app, 'MyCleanerLambdaStack', {
});