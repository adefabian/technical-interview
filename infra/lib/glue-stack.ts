import {
  Code,
  CompressionType,
  Database,
  DataFormat,
  GlueVersion,
  Job,
  JobBookmarksEncryptionMode,
  JobExecutable,
  PythonVersion,
  S3EncryptionMode,
  S3Table,
  Schema,
  SecurityConfiguration,
  StorageParameter,
  WorkerType,
} from "@aws-cdk/aws-glue-alpha";
import * as cdk from "aws-cdk-lib";
import {
  IRole,
  ManagedPolicy,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { IKey } from "aws-cdk-lib/aws-kms";
import { IBucket } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

export interface GlueStackProps extends cdk.StackProps {
  inputBucket: IBucket;
  outputBucket: IBucket;
  bucketKey: IKey;
}

export class GlueStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: GlueStackProps) {
    super(scope, id, props);

    // Glue database
    const database = new Database(this, "GlueDatabase", {
      databaseName: "taxi_data_db",
    });

    // Glue table
    const bronzeTaxiTable = new S3Table(this, "GlueParquetTable", {
      database: database,
      tableName: "bronze_taxi_data",
      bucket: props.outputBucket,
      s3Prefix: "bronze_taxi_data/", // Data will be stored in this prefix within the bucket
      columns: [
        { name: "tpep_pickup_datetime", type: Schema.TIMESTAMP },
        { name: "tpep_dropoff_datetime", type: Schema.TIMESTAMP },
        { name: "passenger_count", type: Schema.DOUBLE },
        { name: "trip_distance", type: Schema.DOUBLE },
        { name: "ratecodeid", type: Schema.DOUBLE },
        { name: "store_and_fwd_flag", type: Schema.STRING },
        { name: "pulocationid", type: Schema.INTEGER },
        { name: "dolocationid", type: Schema.INTEGER },
        { name: "payment_type", type: Schema.BIG_INT },
        { name: "fare_amount", type: Schema.DOUBLE },
        { name: "extra", type: Schema.DOUBLE },
        { name: "mta_tax", type: Schema.DOUBLE },
        { name: "tip_amount", type: Schema.DOUBLE },
        { name: "tolls_amount", type: Schema.DOUBLE },
        { name: "improvement_surcharge", type: Schema.DOUBLE },
        { name: "total_amount", type: Schema.DOUBLE },
        { name: "congestion_surcharge", type: Schema.DOUBLE },
        { name: "airport_fee", type: Schema.DOUBLE },
        { name: "pickup_location", type: Schema.STRING },
        { name: "drop_off_location", type: Schema.STRING },
      ],
      partitionKeys: [{ name: "vendorid", type: Schema.STRING }],
      encryptionKey: props.bucketKey,
      dataFormat: DataFormat.PARQUET,
      storedAsSubDirectories: true,
      storageParameters: [
        StorageParameter.compressionType(CompressionType.NONE),
      ],
      parameters: {
        typeOfData: "file",
      },
    });

    const etlRole: IRole = new Role(this, "glue-role", {
      roleName: "glue-etl-role",
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
        ManagedPolicy.fromAwsManagedPolicyName("SecretsManagerReadWrite"),
      ],
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
    });

    props.bucketKey.grantEncryptDecrypt(etlRole);
    props.inputBucket.grantReadWrite(etlRole);
    props.outputBucket.grantReadWrite(etlRole);

    const glueSecurityGroup = new SecurityConfiguration(
      this,
      "securityGroupGlue",
      {
        securityConfigurationName: "security-group-glue",
        s3Encryption: {
          mode: S3EncryptionMode.KMS,
          kmsKey: props.bucketKey,
        },
        jobBookmarksEncryption: {
          mode: JobBookmarksEncryptionMode.CLIENT_SIDE_KMS,
          kmsKey: props.bucketKey,
        },
      }
    );
    //
    // const glueWheelDeploymentSrcDir: string = path.join(
    //   __dirname,
    //   "etl-scripts"
    // );
    // const glueSources: ISource[] = [
    //   Source.asset(glueWheelDeploymentSrcDir, {
    //     bundling: {
    //       image: DockerImage.fromRegistry("alpine"),
    //       local: {
    //         tryBundle(outputDir: string) {
    //           execSync(
    //             `python ${path.join(
    //               glueWheelDeploymentSrcDir,
    //               "setup.py"
    //             )} bdist_wheel --dist-dir=${path.join(outputDir)}`
    //           );
    //           return true;
    //         },
    //       },
    //     },
    //   }),
    // ];
    //
    // const bucketDeploymentRole = new Role(this, "bucket_deploymentRole", {
    //   roleName: "bucketDeploymentRole",
    //   managedPolicies: [ManagedPolicy.fromAwsManagedPolicyName(
    //       "service-role/AWSLambdaBasicExecutionRole"
    //   )],
    //   assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
    // });
    //
    // props.inputBucket.grantReadWrite(bucketDeploymentRole);
    // props.bucketKey.grantEncryptDecrypt(bucketDeploymentRole);
    //
    // const glueWheelDeploymentSrcDirLocal: string = path.join(
    //   __dirname,
    //   "etl-scripts"
    // );
    // new BucketDeployment(this, "glue_dependency_deployment", {
    //   sources: glueSources,
    //   destinationBucket: props.inputBucket,
    //   serverSideEncryptionAwsKmsKeyId: props.bucketKey.keyId,
    //   destinationKeyPrefix: "scripts",
    //   prune: false,
    //   role: bucketDeploymentRole
    // });

    const bronzeTaxiDataIngestionJob = new Job(this, "bronze_taxi_job", {
      jobName: "bronze_taxi_data_ingestion",
      description: "Ingests the taxi data into the delta lake.",
      workerType: WorkerType.STANDARD,
      workerCount: 3,
      executable: JobExecutable.pythonStreaming({
        script: Code.fromAsset(
          `${__dirname}/etl-scripts/taxi_trip_ingestion.py`
        ),
        pythonVersion: PythonVersion.THREE,
        glueVersion: GlueVersion.V4_0,
        extraPythonFiles: [
          Code.fromBucket(
            props.inputBucket,
            "scripts/shared-0.1-py3-none-any.whl"
          ),
        ],
      }),
      defaultArguments: {
        "--datalake-formats": "delta",
        "--conf":
          "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension " +
          "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog " +
          "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        "--INPUT_PATH": `s3://${props.inputBucket.bucketName}/raw_taxi_data/`,
        "--TAXI_DB": database.databaseName,
        "--BRONZE_TAXI_TABLE": bronzeTaxiTable.tableName,
        "--OUTPUT_PATH": `s3://${props.outputBucket.bucketName}/bronze_taxi_data/`,
        "--CHECKPOINT_LOCATION": `s3://${props.outputBucket.bucketName}/checkpoints/bronze_taxi_data/`,
      },
      securityConfiguration: glueSecurityGroup,
      role: etlRole,
    });

    const silverTaxiDataIngestionJob = new Job(this, "silver_taxi_job", {
      jobName: "silver_taxi_data_ingestion",
      description: "Processes the raw taxi data and extracts relevant info.",
      workerType: WorkerType.STANDARD,
      workerCount: 3,
      executable: JobExecutable.pythonStreaming({
        script: Code.fromAsset(`${__dirname}/etl-scripts/taxi_trip_filter.py`),
        pythonVersion: PythonVersion.THREE,
        glueVersion: GlueVersion.V4_0,
        extraPythonFiles: [
          Code.fromBucket(
            props.inputBucket,
            "scripts/shared-0.1-py3-none-any.whl"
          ),
        ],
      }),
      defaultArguments: {
        "--datalake-formats": "delta",
        "--conf":
          "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension " +
          "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog " +
          "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore " +
          "--conf spark.jars.packages=com.amazon.deequ:deequ:2.0.7-spark-3.3 " +
          "--conf spark.jars.excludes=net.sourceforge.f2j:arpack_combined_all",
        "--INPUT_PATH": `s3://${props.outputBucket.bucketName}/bronze_taxi_data/`,
        "--OUTPUT_PATH": `s3://${props.outputBucket.bucketName}/silver_taxi_data/`,
        "--CHECKPOINT_LOCATION": `s3://${props.outputBucket.bucketName}/checkpoints/silver_taxi_data/`,
        "--additional-python-modules": "pydeequ",
      },
      securityConfiguration: glueSecurityGroup,
      role: etlRole,
    });

    const goldTaxiDataIngestionJob = new Job(this, "gold_taxi_job", {
      jobName: "gold_taxi_data_ingestion",
      description: "Processes the silver taxi data and combines it with the weather data.",
      workerType: WorkerType.STANDARD,
      workerCount: 3,
      executable: JobExecutable.pythonStreaming({
        script: Code.fromAsset(`${__dirname}/etl-scripts/taxi_trip_agg.py`),
        pythonVersion: PythonVersion.THREE,
        glueVersion: GlueVersion.V4_0,
        extraPythonFiles: [
          Code.fromBucket(
              props.inputBucket,
              "scripts/shared-0.1-py3-none-any.whl"
          ),
        ],
      }),
      defaultArguments: {
        "--datalake-formats": "delta",
        "--conf":
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension " +
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog " +
            "--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        "--INPUT_PATH": `s3://${props.outputBucket.bucketName}/silver_taxi_data/`,
        "--INOUT_PATH_WEATHER": `s3://${props.inputBucket.bucketName}/weather_data/`,
        "--OUTPUT_PATH": `s3://${props.outputBucket.bucketName}/gold_taxi_data/`,
        "--CHECKPOINT_LOCATION": `s3://${props.outputBucket.bucketName}/checkpoints/gold_taxi_data/`,
      },
      securityConfiguration: glueSecurityGroup,
      role: etlRole,
    });
  }
}
