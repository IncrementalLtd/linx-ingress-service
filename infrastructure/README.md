# Linx Ingress Infrastructure (CDK)

TypeScript AWS CDK app (Node 22) for Linx data **ingress, store and retrieval**.

This stack provisions one Kinesis Data Stream per source MQ topic and grants the
EC2/SSM instance role permission to publish to them.

## Streams

Each stream is named after the unique segment of its source topic
(`NR.ALG.<UNIQUE>.TO.INC01`):

| Stream name | Source topic              |
| ----------- | ------------------------- |
| `VSCSLEI`   | `NR.ALG.VSCSLEI.TO.INC01`   |
| `TDCSLEI`   | `NR.ALG.TDCSLEI.TO.INC01`   |
| `TMCSTCLEI` | `NR.ALG.TMCSTCLEI.TO.INC01` |
| `TMCSTRLEI` | `NR.ALG.TMCSTRLEI.TO.INC01` |
| `TMCSIDLEI` | `NR.ALG.TMCSIDLEI.TO.INC01` |
| `TMCSTMLEI` | `NR.ALG.TMCSTMLEI.TO.INC01` |

All streams use **on-demand** capacity mode with a 24-hour retention period.

## Publish permissions

The role
`arn:aws:iam::141288794635:role/AmazonSSMRoleForInstancesQuickSetup`
is granted write access (`kinesis:PutRecord`, `kinesis:PutRecords`,
`kinesis:ListShards`) to every stream.

## Prerequisites

- Node.js 22+
- AWS credentials for account `141288794635` (the role's account)
- The target region/account must be CDK-bootstrapped:
  `npx cdk bootstrap aws://141288794635/<region>`

## Usage

```bash
cd infrastructure
npm install

# Preview the CloudFormation template
npm run synth

# Show the diff against deployed state
npm run diff

# Deploy
npm run deploy

# Tear down
npm run destroy
```

The account/region come from your active AWS CLI profile
(`CDK_DEFAULT_ACCOUNT` / `CDK_DEFAULT_REGION`). Set them explicitly if needed:

```bash
AWS_REGION=eu-west-2 npm run deploy
```
