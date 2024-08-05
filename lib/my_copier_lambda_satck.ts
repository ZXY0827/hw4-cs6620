import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda  from 'aws-cdk-lib/aws-lambda';
import * as iam  from 'aws-cdk-lib/aws-iam';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as lambda_event_sources from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import { RemovalPolicy } from 'aws-cdk-lib';

export class MyCopierLambdaStack extends cdk.Stack {
  public readonly sourceBucketName: string;
  public readonly destinationBucketName: string;
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const sourceBucket = new s3.Bucket(this, 'source', { removalPolicy: RemovalPolicy.DESTROY });
	const destinationBucket =new s3.Bucket(this, 'destination',{ removalPolicy: RemovalPolicy.DESTROY });
		
	this.sourceBucketName = sourceBucket.bucketName;
	this.destinationBucketName = destinationBucket.bucketName;
		
	new cdk.CfnOutput(this, 'DestinationBucketNameExport', {
        value: destinationBucket.bucketName,
        exportName: 'DestinationBucketName'
    });
    
     // Create SNS topic for fanout
    const fanoutTopic = new sns.Topic(this, 'FanoutTopic');
    
    // Create an SQS queue to handle copy 
    const copierQueue = new sqs.Queue(this, 'CopierQueue', {visibilityTimeout:cdk.Duration.seconds(50)});
    fanoutTopic.addSubscription(new subscriptions.SqsSubscription(copierQueue));
    
    // Create an SQS queue to handle log 
    const logQueue = new sqs.Queue(this, 'LogQueue',{visibilityTimeout:cdk.Duration.seconds(50)});
    //fanoutTopic.addSubscription(new subscriptions.SqsSubscription(logQueue));

    const lambdaExecutionRole = new iam.Role(this, 'LambdaExecutionRole', {
          assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    lambdaExecutionRole.addToPolicy(new iam.PolicyStatement({
          actions: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject','s3:ListBucket','s3:HeadObject'],
          resources: [sourceBucket.bucketArn, sourceBucket.bucketArn + '/*', destinationBucket.bucketArn, destinationBucket.bucketArn + '/*'],
          effect: iam.Effect.ALLOW
    }));
    
    lambdaExecutionRole.addToPolicy(new iam.PolicyStatement({
          actions: ['sns:Publish'],
          resources: [fanoutTopic.topicArn],  
          effect: iam.Effect.ALLOW
    }));

    lambdaExecutionRole.addToPolicy(new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:GetQueueUrl', 'sqs:SendMessage'],
        resources: [copierQueue.queueArn], // 使用 copierQueue 的资源 ARN
        effect: iam.Effect.ALLOW
    }));
    
    // Authorize logLambda to process logQueue messages
    lambdaExecutionRole.addToPolicy(new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes', 'sqs:GetQueueUrl','sqs:SendMessage'],
        resources: [logQueue.queueArn], 
        effect: iam.Effect.ALLOW
    }));
        
    const copierLogGroup = new logs.LogGroup(this, 'CopierLogs', {
          logGroupName: '/aws/lambda/CopierLogs',
          removalPolicy: cdk.RemovalPolicy.DESTROY // Automatically remove the log group on stack deletion
    });
    
    const copierLambda = new lambda.Function(this, 'copier', {
	        runtime: lambda.Runtime.PYTHON_3_8,
		      handler: 'copier_lambda.copier_handler',
		      role: lambdaExecutionRole,
		      environment: {
                SOURCE_BUCKET_NAME: sourceBucket.bucketName,
                DESTINATION_BUCKET_NAME: destinationBucket.bucketName,
                LOG_QUEUE_URL: logQueue.queueUrl,
                COPIER_QUEUE_URL: copierQueue.queueUrl
                // LOG_GROUP_NAME: copierLogGroup.logGroupName
              },
		      code: lambda.Code.fromAsset('copier'),
		      logGroup: copierLogGroup
    });
    
    const logLambda = new lambda.Function(this, 'log_handler', {
          runtime: lambda.Runtime.PYTHON_3_8,
          handler: 'log_lambda.log_handler',
          role: lambdaExecutionRole,
          environment: {
              // LOG_GROUP_NAME: copierLogGroup.logGroupName,
              DESTINATION_BUCKET_NAME: destinationBucket.bucketName,
              LOG_QUEUE_URL: logQueue.queueUrl
          },
          code: lambda.Code.fromAsset('copier'),
          logGroup: copierLogGroup
    });
    
    //sourceBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.LambdaDestination(copierLambda));
    // sourceBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.LambdaDestination(logLambda), { prefix: 'to-log/' });

    sourceBucket.grantRead(copierLambda);
    destinationBucket.grantWrite(copierLambda);
    
    copierLogGroup.grantWrite(copierLambda)
    copierLogGroup.grantWrite(logLambda);


    copierLambda.addEventSource(new lambda_event_sources.SqsEventSource(copierQueue, {batchSize: 1}));
    logLambda.addEventSource(new lambda_event_sources.SqsEventSource(logQueue, {batchSize: 1}));

    // Authorize Lambda functions to process their own SQS messages
    copierQueue.grantConsumeMessages(copierLambda);
    logQueue.grantConsumeMessages(logLambda);
    
    // Notify S3 events to SNS topics
    sourceBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.SnsDestination(fanoutTopic));

    const sizeMetricFilter = new logs.MetricFilter(this, 'SizeMetricSNS', {
          logGroup: copierLogGroup,
          metricNamespace: 'ObjectsSNS',
          metricName: 'TotalSizeSNS',
          filterPattern: logs.FilterPattern.literal('[info=Total, ... , size_value, unit="bytes"]'), 
          metricValue: '$size_value'
    });
  }
}