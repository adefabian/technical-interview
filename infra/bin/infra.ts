#!/opt/homebrew/opt/node/bin/node
import * as cdk from "aws-cdk-lib";
import "source-map-support/register";
import { GlueStack } from "../lib/glue-stack";
import { KmsStack } from "../lib/kms-keys";
import { S3Stack } from "../lib/s3-stack";
import { VpcStack } from "../lib/vpc-stack";

const app = new cdk.App();
const vpcStack: VpcStack = new VpcStack(app, "vpc_stack", {
  stackName: "vpc-stack",
});

const kmsStack: KmsStack = new KmsStack(app, "kms_stack", {
  stackName: "kms-stack",
});

const s3Stack: S3Stack = new S3Stack(app, "s3_stack", {
  stackName: "s3-stack",
  bucket_encryption_key: kmsStack.bucket_key,
});

const glueStack: GlueStack = new GlueStack(app, "glue_stack", {
  stackName: "glue-stack",
  inputBucket: s3Stack.s3_data_lake_bucket,
  outputBucket: s3Stack.s3_bucket,
  bucketKey: kmsStack.bucket_key,
});
