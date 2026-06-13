import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as path from 'path';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { HttpJwtAuthorizer } from 'aws-cdk-lib/aws-apigatewayv2-authorizers';

const REGION = 'eu-west-2';
const ACCOUNT = '093711202389';

// Cognito (dev) user pool that issues the JWTs accepted by the query API.
const COGNITO_DEV_POOL_ID = 'eu-west-2_VrlqkaG0h';
const COGNITO_DEV_CLIENT_ID = 'smqqult962v4k9pb4qjvf4s12';

/** Each Kinesis stream consumed, with the message-type code stored as the table PK. */
const CONSUMERS = [
  { stream: 'TMCSTCLEI', messageType: 'TMCSTC' },
  { stream: 'TMCSIDLEI', messageType: 'TMCSID' },
  { stream: 'TMCSTMLEI', messageType: 'TMCSTM' },
];

/**
 * Linx data store + retrieval: a DynamoDB table fed by per-stream consumer
 * Lambdas, plus an HTTP API to query stored messages by type / run date / time.
 */
export class LinxProcessingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // PK = messageType, SK = `${runDate}#${messageDateTime}#${messageId}`.
    // Lets you query a message type by run date, then by a date-time range.
    const table = new dynamodb.Table(this, 'LinxMessagesTable', {
      tableName: 'LinxMessages',
      partitionKey: { name: 'messageType', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Bundle with esbuild; the AWS SDK v3 is already in the Node 22 runtime.
    const bundling = { externalModules: ['@aws-sdk/*'], minify: true, target: 'node22' };

    // One consumer Lambda per stream.
    for (const c of CONSUMERS) {
      const fn = new NodejsFunction(this, `${c.messageType}Consumer`, {
        entry: path.join(__dirname, '..', 'lambda', 'consumer.ts'),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_22_X,
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.seconds(60),
        memorySize: 256,
        environment: { TABLE_NAME: table.tableName, MESSAGE_TYPE: c.messageType },
        bundling,
      });

      table.grantWriteData(fn);

      const stream = kinesis.Stream.fromStreamArn(
        this,
        `${c.messageType}Stream`,
        `arn:aws:kinesis:${REGION}:${ACCOUNT}:stream/${c.stream}`,
      );

      fn.addEventSource(
        new KinesisEventSource(stream, {
          // TRIM_HORIZON so we pick up anything already in the (24h) stream,
          // not just records published after the trigger is created.
          startingPosition: lambda.StartingPosition.TRIM_HORIZON,
          batchSize: 100,
          maxBatchingWindow: cdk.Duration.seconds(10),
          retryAttempts: 5,
          bisectBatchOnError: true,
          reportBatchItemFailures: true,
        }),
      );
    }

    // Query API Lambda.
    const apiFn = new NodejsFunction(this, 'QueryApiFn', {
      entry: path.join(__dirname, '..', 'lambda', 'api.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      environment: { TABLE_NAME: table.tableName },
      bundling,
    });

    table.grantReadData(apiFn);

    const httpApi = new apigwv2.HttpApi(this, 'LinxQueryApi', {
      apiName: 'linx-messages-query',
      corsPreflight: {
        allowOrigins: ['*'],
        allowMethods: [apigwv2.CorsHttpMethod.GET],
        allowHeaders: ['authorization', 'content-type'],
      },
    });

    // JWT authorizer backed by the Cognito dev user pool. Callers must send a
    // valid token (issued for this app client) in the Authorization header.
    const authorizer = new HttpJwtAuthorizer(
      'LinxJwtAuthorizer',
      `https://cognito-idp.${REGION}.amazonaws.com/${COGNITO_DEV_POOL_ID}`,
      {
        jwtAudience: [COGNITO_DEV_CLIENT_ID],
        identitySource: ['$request.header.Authorization'],
      },
    );

    httpApi.addRoutes({
      path: '/messages',
      methods: [apigwv2.HttpMethod.GET],
      integration: new HttpLambdaIntegration('QueryIntegration', apiFn),
      authorizer,
    });

    new cdk.CfnOutput(this, 'QueryApiUrl', {
      value: `${httpApi.apiEndpoint}/messages`,
      description: 'GET /messages?messageType=&runDate=&from=&to=',
    });
    new cdk.CfnOutput(this, 'LinxMessagesTableName', { value: table.tableName });
  }
}
