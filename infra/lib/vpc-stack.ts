import * as cdk from "aws-cdk-lib";
import { IVpc, SubnetType, Vpc } from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";

export class VpcStack extends cdk.Stack {
  public readonly vpc: IVpc;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new Vpc(this, "playground_vpc", {
      natGateways: 1,
      maxAzs: 3,
      subnetConfiguration: [
        {
          name: "public",
          subnetType: SubnetType.PUBLIC,
          mapPublicIpOnLaunch: false,
        },
        {
          name: "private",
          subnetType: SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });
  }
}
