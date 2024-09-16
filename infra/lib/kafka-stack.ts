import * as cdk from "aws-cdk-lib";
import {
  ISecurityGroup,
  IVpc,
  SecurityGroup,
  SubnetType,
} from "aws-cdk-lib/aws-ec2";
import * as msk from "aws-cdk-lib/aws-msk";
import { Construct } from "constructs";

export interface KafkaStackProps extends cdk.StackProps {
  vpc: IVpc;
}

export class KafkaStack extends cdk.Stack {
  public kafkaCluster: msk.CfnCluster;
  public kafkaSecurityGroup: ISecurityGroup;
  public kafkaClientSecurityGroup: ISecurityGroup;
  constructor(scope: Construct, id: string, props: KafkaStackProps) {
    super(scope, id, props);

    this.kafkaSecurityGroup = new SecurityGroup(this, "kafkaSecurityGroup", {
      securityGroupName: "kafka_security_group",
      allowAllOutbound: true,
      vpc: props.vpc,
    });

    this.kafkaClientSecurityGroup = new SecurityGroup(
      this,
      "kafkaClientSecurityGroup",
      {
        securityGroupName: "kafka_client_security_group",
        allowAllOutbound: true,
        vpc: props.vpc,
      }
    );

    this.kafkaCluster = new msk.CfnCluster(this, "kafkaCluster", {
      brokerNodeGroupInfo: {
        securityGroups: [
          this.kafkaSecurityGroup.securityGroupId,
          this.kafkaClientSecurityGroup.securityGroupId,
        ],
        clientSubnets: props.vpc.selectSubnets({
          subnetType: SubnetType.PUBLIC,
        }).subnetIds,
        instanceType: "kafka.t3.small",
        storageInfo: {
          ebsStorageInfo: {
            volumeSize: 5,
          },
        },
      },
      clientAuthentication: {
        sasl: {
          iam: {
            enabled: true,
          },
        },
      },
      clusterName: "transactions_kafka_cluster",
      kafkaVersion: "2.7.0",
      numberOfBrokerNodes: 2,
    });
  }
}
