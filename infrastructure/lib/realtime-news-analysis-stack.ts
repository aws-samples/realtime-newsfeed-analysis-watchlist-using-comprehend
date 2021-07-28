// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from '@aws-cdk/core';
import {CfnParameter, Duration} from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as es from '@aws-cdk/aws-elasticsearch';
import * as sqs from '@aws-cdk/aws-sqs';
import * as sns from '@aws-cdk/aws-sns';
import * as subscriptions from '@aws-cdk/aws-sns-subscriptions';
import * as iam from '@aws-cdk/aws-iam'
import {Effect} from '@aws-cdk/aws-iam'
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as rds from '@aws-cdk/aws-rds';
import * as kms from '@aws-cdk/aws-kms';
import * as ssm from '@aws-cdk/aws-ssm';


export class RealtimeNewsAnalysisStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const rnaKey = new kms.Key(this, 'RNAKey', { enableKeyRotation: true});

    const incoming_news_queue = new sqs.Queue(this, 'Queue', {
      encryption: sqs.QueueEncryption.KMS_MANAGED,
      encryptionMasterKey: rnaKey,
      visibilityTimeout: Duration.minutes(10)
    });

    const match_topic = new sns.Topic(this, 'NewsfeedMatchTopic', {
      displayName: 'Newsfeed Match subscription topic',
      masterKey: rnaKey
    });

    const emailAddress = new CfnParameter(this, 'notification-email-param');

    match_topic.addSubscription(new subscriptions.EmailSubscription(emailAddress.valueAsString));
    match_topic.addSubscription(new subscriptions.EmailSubscription("zshabat@amazon.com"));

    // create role for lambda
    // PLEASE NOTE - Please note the permissions above are coarse grained for the prototype. in Production system this will be fine-grained
    const lambda_role = new iam.Role(this, 'lambdaRole', {
      assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal('sns.amazonaws.com'),
          new iam.ServicePrincipal('lambda.amazonaws.com')
      )
    });
    //lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AWSLambdaBasicExecutionRole'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this,'AWSLambdaBasicExecutionRole',"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"))

    //lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AWSLambdaVPCAccessExecutionRole'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this,'AWSLambdaVPCAccessExecutionRole',"arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('SecretsManagerReadWrite'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSQSFullAccess'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSFullAccess'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('ComprehendFullAccess'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonRDSDataFullAccess'))


    //lambda_role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSRole'))
    lambda_role.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this,'AmazonSNSRole',"arn:aws:iam::aws:policy/service-role/AmazonSNSRole"))

    lambda_role.addToPolicy(new iam.PolicyStatement({
      resources: [rnaKey.keyArn],
      actions: [
        'kms:Create*',
        'kms:Describe*',
        'kms:Decrypt',
        'kms:Encrypt',
        'kms:GenerateDataKey',
        'kms:ReEncryptFrom',
        'kms:ReEncryptTo',
        'kms:Enable*',
        'kms:List*',
        'kms:Put*'
      ],
      effect: Effect.ALLOW
    }));


    const newsfeed_bucket = new s3.Bucket(this, 'NewsfeedBucket', {
      encryptionKey: rnaKey
    });

    newsfeed_bucket.addToResourcePolicy(
        new iam.PolicyStatement({
          resources: [
            newsfeed_bucket.arnForObjects("*")
          ],
          actions: ["s3:PutObject"],
          principals: [new iam.ArnPrincipal(lambda_role.roleArn)]
        })
    );

    const vpc = new ec2.Vpc(this, 'AuroraDBVPC');

    const cluster = new rds.ServerlessCluster(this, 'aurora-postgres-cluster', {
      engine: rds.DatabaseClusterEngine.AURORA_POSTGRESQL,
      parameterGroup: rds.ParameterGroup.fromParameterGroupName(this, 'ParameterGroup', 'default.aurora-postgresql10'),
      vpc,
      scaling: {
        autoPause: Duration.minutes(10), // default is to pause after 5 minutes of idle time
        minCapacity: rds.AuroraCapacityUnit.ACU_8, // default is 2 Aurora capacity units (ACUs)
        maxCapacity: rds.AuroraCapacityUnit.ACU_32, // default is 16 Aurora capacity units (ACUs)
      },
      enableDataApi: true,
      storageEncryptionKey: rnaKey
    });


    // create secret manager
    const cfn_secret = new secretsmanager.CfnSecret(
        this, 'RNASecret', {
          name:'RNASecret',
          kmsKeyId:rnaKey.keyId,
          secretString : JSON.stringify({
            "RNA-lambda-role": lambda_role.roleArn,
            "newsfeed-bucket": newsfeed_bucket.bucketName,
            "incoming-newsfeed-queue" : incoming_news_queue.queueName,
            "incoming-newsfeed-queue-arn" : incoming_news_queue.queueArn,
            "sns-notification-topic": match_topic.topicArn,
            "db-cluster-arn": cluster.clusterArn,
            "db-secret": cluster.secret?.secretArn
          })
        }
    )

    new cdk.CfnOutput(this, 'SecretName', {
      value: cfn_secret.ref
    });

    new cdk.CfnOutput(this, 'LambdaRole', {
      value: lambda_role.roleArn
    });

    new cdk.CfnOutput(this, 'newsfeed-bucket', {
      value: newsfeed_bucket.bucketArn
    });

    new cdk.CfnOutput(this, 'incoming-newsfeed-queue', {
      value: incoming_news_queue.queueArn
    });

    new cdk.CfnOutput(this, 'sns-notification-topic', {
      value: match_topic.topicArn
    });

  }
}

// cdk deploy --parameters notificationemailparam=zshabat@amazon.com