# Taxi Analytics platform

This project uses CDK development with TypeScript, to deploy the taxi analytics platform on AWS.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template

## Stacks

- vpc-stack: deploys the networking resources
- kms-stack: deploys the managed keys used for decrypting/encrypting
- s3-stack: deploys the s3 buckets
- glue-stack deploys the etl jobs
