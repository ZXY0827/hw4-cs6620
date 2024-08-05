import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda  from 'aws-cdk-lib/aws-lambda';
import * as iam  from 'aws-cdk-lib/aws-iam';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda_event_sources from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import { RemovalPolicy } from 'aws-cdk-lib';

export class MyCleanerLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const destinationBucketName = cdk.Fn.importValue('DestinationBucketName');
    const destinationBucket = s3.Bucket.fromBucketName(this, 'ImportedDestinationBucket', destinationBucketName);
    
    // SNS topic for alarms
    const alarmTopic = new sns.Topic(this, 'AlarmTopic');
    
    const cleanerQueue = new sqs.Queue(this, 'CleanerQueue',{visibilityTimeout:cdk.Duration.seconds(50)});
    
    alarmTopic.addSubscription(new subscriptions.SqsSubscription(cleanerQueue));
    
    const cleanerLambda = new lambda.Function(this, 'Cleaner', {
            runtime: lambda.Runtime.PYTHON_3_8,
            handler: 'cleaner_lambda.lambda_handler',
            code: lambda.Code.fromAsset('cleaner'),
            environment: {
              'DESTINATION_BUCKET_NAME': destinationBucketName 
            },
    });
    
    cleanerLambda.addToRolePolicy(new iam.PolicyStatement({
        actions: ["s3:ListBucket",
                  "s3:GetObject",
                  "s3:DeleteObject",
                  "logs:CreateLogStream",
                  "logs:PutLogEvents",
                  "logs:CreateLogGroup"],
        resources: [
            `arn:aws:s3:::${destinationBucketName}`, 
            `arn:aws:s3:::${destinationBucketName}/*`
        ]
    }));

      // Ensure access to the specific SNS topic and SQS queue
    cleanerLambda.addToRolePolicy(new iam.PolicyStatement({
        actions: ["sns:Publish"],
        resources: [alarmTopic.topicArn]
    }));
    cleanerLambda.addToRolePolicy(new iam.PolicyStatement({
        actions: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
        resources: [cleanerQueue.queueArn]
    }));
        
    // Set up an SQS queue as an event source for your Lambda function
    cleanerLambda.addEventSource(new lambda_event_sources.SqsEventSource(cleanerQueue, {batchSize: 1}));
    
    destinationBucket.grantReadWrite(cleanerLambda);
    
    // Create CloudWatch alarm
    const alarm = new cloudwatch.Alarm(this, 'TotalSizeAlarmSNS', {
      metric: new cloudwatch.Metric({
        namespace: 'ObjectsSNS',
        metricName: 'TotalSizeSNS',
        statistic: 'Maximum',
        period: cdk.Duration.seconds(60)
      }),
      threshold: 3072,  //  3 * 1024 
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
    });
    
    // Add SNS topic as the action for the alarm
    alarm.addAlarmAction(new actions.SnsAction(alarmTopic));

    // Set the cleaner Lambda function as the alert's action
    //alarm.addAlarmAction(new actions.LambdaAction(cleanerLambda));
  }
}
