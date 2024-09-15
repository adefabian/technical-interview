import * as cdk from "aws-cdk-lib";
import { RemovalPolicy } from "aws-cdk-lib";
import { IKey } from "aws-cdk-lib/aws-kms";
import { Bucket, IBucket } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

export interface S3StackProps extends cdk.StackProps {
  bucket_encryption_key: IKey;
}

export class S3Stack extends cdk.Stack {
  public readonly s3_bucket: IBucket;
  public readonly s3_data_lake_bucket: IBucket;

  constructor(scope: Construct, id: string, props: S3StackProps) {
    super(scope, id, props);

    this.s3_bucket = new Bucket(this, "s3_bucket", {
      encryptionKey: props.bucket_encryption_key,
      bucketName: "technical-interview.analytics-platform",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.s3_data_lake_bucket = new Bucket(this, "s3_data_lake_bucket", {
      encryptionKey: props.bucket_encryption_key,
      bucketName: "technical-interview.data-lake",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
  }
}
