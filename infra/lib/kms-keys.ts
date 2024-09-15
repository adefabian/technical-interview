import * as cdk from "aws-cdk-lib";
import { Key } from "aws-cdk-lib/aws-kms";
import { Construct } from "constructs";

export class KmsStack extends cdk.Stack {
  public readonly bucket_key: Key;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    this.bucket_key = new Key(this, "s3_bucket_key", {
      description: "Used for encrypting files stored inside s3.",
      alias: "s3_bucket_key",
    });
  }
}
