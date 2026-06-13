import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as iam from 'aws-cdk-lib/aws-iam';

/**
 * AWS account that hosts the Linx ingress producer (the EC2/SSM instance role
 * arn:aws:iam::141288794635:role/AmazonSSMRoleForInstancesQuickSetup).
 *
 * The streams in this stack live in a *different* account, so cross-account
 * publish requires both:
 *   1. an identity policy on the producer side  -> handled in that account
 *   2. a resource policy on each stream          -> handled here
 */
const PRODUCER_ACCOUNT_ID = '141288794635';

/**
 * Source MQ topics. We name each Kinesis data stream after the unique segment
 * of the topic (the third dotted token in NR.ALG.<UNIQUE>.TO.INC01).
 */
const STREAM_NAMES: string[] = [
  'VSCSLEI', // NR.ALG.VSCSLEI.TO.INC01
  'TDCSLEI', // NR.ALG.TDCSLEI.TO.INC01
  'TMCSTCLEI', // NR.ALG.TMCSTCLEI.TO.INC01
  'TMCSTRLEI', // NR.ALG.TMCSTRLEI.TO.INC01
  'TMCSIDLEI', // NR.ALG.TMCSIDLEI.TO.INC01
  'TMCSTMLEI', // NR.ALG.TMCSTMLEI.TO.INC01
];

export class LinxIngressStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    for (const name of STREAM_NAMES) {
      // Construct id is just `name` (not `${name}Stream`) on purpose: it changes
      // the CloudFormation logical id so CFN creates a fresh provisioned stream
      // instead of trying to update the original on-demand one in place — which
      // Kinesis blocks (can't halve 4 -> 1 in one step, and capacity-mode changes
      // are capped at twice per 24h). The original VSCSLEI stream is deleted
      // out-of-band first so the name is free; see the infrastructure README.
      const stream = new kinesis.Stream(this, name, {
        streamName: name,
        // Provisioned single shard — 1 MB/s, 1,000 records/s — ample for these
        // low-volume feeds and cheaper than on-demand's 4-shard floor.
        streamMode: kinesis.StreamMode.PROVISIONED,
        shardCount: 1,
        retentionPeriod: cdk.Duration.hours(24),
        // Data is not sensitive, and server-side encryption with the AWS-managed
        // key blocks cross-account producers (they can't be granted kms access to
        // the aws/kinesis key). Disable it so the producer role can PutRecords.
        encryption: kinesis.StreamEncryption.UNENCRYPTED,
        // Let CloudFormation delete the stream on removal (the L2 default is
        // Retain) so future teardowns/recreations don't need a manual delete.
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

      // Resource-based policy: allow the producer account to publish.
      // The producer account grants its specific role the matching identity
      // permissions; effective access is the intersection of the two.
      stream.addToResourcePolicy(
        new iam.PolicyStatement({
          sid: 'AllowCrossAccountWrite',
          effect: iam.Effect.ALLOW,
          principals: [new iam.AccountPrincipal(PRODUCER_ACCOUNT_ID)],
          actions: [
            'kinesis:PutRecord',
            'kinesis:PutRecords',
            'kinesis:DescribeStreamSummary',
            'kinesis:ListShards',
          ],
          resources: [stream.streamArn],
        }),
      );

      new cdk.CfnOutput(this, `${name}StreamArn`, {
        value: stream.streamArn,
        description: `ARN of the ${name} Kinesis data stream`,
      });
    }
  }
}
