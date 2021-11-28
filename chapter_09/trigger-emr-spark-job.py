import json
import boto3
import time
import os
from datetime import datetime

currentYear = str(datetime.now().year)
currentMonth = str(datetime.now().month)


PYSPARK_SCRIPT_PATH = os.environ['PYSPARK_SCRIPT_PATH']
S3_OUTPUT_PREFIX = os.environ['S3_OUTPUT_PREFIX']
AWS_REGION = os.environ['REGION']

def lambda_handler(event, context):

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
        
    S3_INPUT_PATH = 's3a://'+bucket_name+'/'+object_key

    conn = boto3.client("emr", region_name=AWS_REGION)        
    cluster_id = conn.run_job_flow(
        Name='Sales-Spark-ETL',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
		AutoScalingRole="EMR_AutoScaling_DefaultRole",
		ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
        VisibleToAllUsers=True,
        LogUri='s3://<path>',
        ReleaseLabel='emr-6.3.0',
		EbsRootVolumeSize=10,
        Instances={
			"Ec2KeyName": "<EC2-KeyPair>",
            "Ec2SubnetId": "subnet-<id>",
			"EmrManagedMasterSecurityGroup": "sg-<id>",
			"EmrManagedSlaveSecurityGroup": "sg-<id>",
			"HadoopVersion": "Amazon 3.2.1",
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },		
		BootstrapActions=[],
        Applications=[{'Name': 'Spark'},{'Name': 'Hadoop'}],
        Configurations=[
			{
				"Classification":"emrfs-site",	
				"Properties":{"fs.s3.consistent.retryPeriodSeconds":"10","fs.s3.consistent":"true","fs.s3.consistent.retryCount":"5","fs.s3.consistent.metadata.tableName":"EmrFSMetadata"}
			},
			{ 'Classification': 'spark-hive-site',
              'Properties': { 
                  'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
		],
        Steps=[
			{
				'Name': 'Spark ETL',
				'ActionOnFailure': 'TERMINATE_CLUSTER',
				'HadoopJarStep': {
					'Jar': 'command-runner.jar',
					'Args': [
						"spark-submit", "--deploy-mode", "cluster",
						"--master", "yarn",  
						PYSPARK_SCRIPT_PATH, S3_INPUT_PATH, S3_OUTPUT_PREFIX+currentYear+"/"+currentMonth+"/"
					]
            }
        }],
    )
    return "Started cluster {}".format(cluster_id)