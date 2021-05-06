#Import libraries
import boto3
import json
import botocore
import time
import os
from random import randint
from functools import wraps
import logging
import sagemaker
import pandas as pd


#Initialise AWS Clients
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

iam_desc = 'IAM Policy for Lambda triggering AWS SageMaker Pipeline'
fcn_desc = 'AWS Lambda function for automatically triggering AWS SageMaker Pipeline'

def create_lambda_fcn(flow_uri, pipeline_name):
    
    #Set variables
    print('Gathering variables ...')
    
    flow_bucket = flow_uri.split('/')[2]
    flow_key = '/'.join(flow_uri.split('/')[3:])
    pipeline_name = pipeline_name
    
    #Create skeleton lambda code
    print('Creating code for AWS Lambda function ...')
    
    
    lambda_code = """
    import json
    import boto3

    s3 = boto3.resource('s3')
    sm = boto3.client('sagemaker')

    def lambda_handler(event, context):

        #Check version of Boto3 - It must be at least 1.16.55
        print(f"The version of Boto3 is {boto3.__version__}")

        #Get location for where the new data (csv) file was uploaded
        data_bucket = event['Records'][0]['s3']['bucket']['name']
        data_key = event['Records'][0]['s3']['object']['key']
        print(f"A new file named {data_key} was just uploaded to Amazon S3 in {data_bucket}")

        #Update values for where Data Wrangler .flow is saved
        flow_bucket = '%(flow_bucket)s'
        flow_key = '%(flow_key)s'
        pipeline_name = '%(pipeline_name)s'
        execution_display = f"{data_key.split('/')[-1].replace('_','').replace('.csv','')}"


        #Get .flow file from Amazon S3
        get_object = s3.Object(flow_bucket,flow_key)
        get_flow = get_object.get()

        #Read, update and save the .flow file
        flow_content = json.loads(get_flow['Body'].read())
        flow_content['nodes'][0]['parameters']['dataset_definition']['name'] = data_key.split('/')[-1]
        flow_content['nodes'][0]['parameters']['dataset_definition']['s3ExecutionContext']['s3Uri'] = f"s3://{data_bucket}/{data_key}"
        new_flow_key = flow_key.replace('.flow', '-' + data_key.split('/')[-1].replace('.csv','') + '.flow')
        new_flow_uri = f"s3://{flow_bucket}/{new_flow_key}"
        put_object = s3.Object(flow_bucket,new_flow_key)
        put_flow = put_object.put(Body=json.dumps(flow_content))


        #Start the pipeline execution
        start_pipeline = sm.start_pipeline_execution(
                        PipelineName=pipeline_name,
                        PipelineExecutionDisplayName=f"{data_key.split('/')[-1].replace('_','').replace('.csv','')}",
                        PipelineParameters=[
                            {
                                'Name': 'InputFlow',
                                'Value': new_flow_uri
                            },
                        ],
                        PipelineExecutionDescription=data_key
                        )
        print(start_pipeline)


        return('SageMaker Pipeline has been successfully executed')
    """ % locals()
   
    #Update success status
    print('SUCCESS: Successfully created code for AWS Lambda function!')
    
    return lambda_code
        
# Define IAM Trust Policy for Lambda's role
iam_trust_policy = {
'Version': '2012-10-17',
'Statement': [
  {
    'Effect': 'Allow',
    'Principal': {
      'Service': 'lambda.amazonaws.com'
    },
    'Action': 'sts:AssumeRole'
  }
]
}


#Define function to allow Amazon S3 to trigger AWS Lambda
def allow_s3(fcn_name,bucket_arn,account_num):
    print('Adding permissions to Amazon S3 ...')
    response = lambda_client.add_permission(
            FunctionName=fcn_name,
            StatementId=f"S3-Trigger-Lambda-{int(time.time())}",
            Action='lambda:InvokeFunction',
            Principal= 's3.amazonaws.com',
            SourceArn=bucket_arn,
            SourceAccount=account_num
        )
    print('SUCCESS: Successfully added permissions to Amazon S3!')

        

def add_permissions(name):
    print("Adding permissions to AWS Lambda function's IAM role ...")
    add_execution_role = iam_client.attach_role_policy(
            RoleName=name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
    
    add_execution_role = iam_client.attach_role_policy(
                RoleName=name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonSageMakerFullAccess'
            )
    print("SUCCESS: Successfully added permissions AWS Lambda function's IAM role!")


def create_role(role_name):
    print('Creating an IAM role for AWS Lambda function ...')
    create_iam_role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(iam_trust_policy),
        Description=iam_desc
        )
    print('SUCCESS: Successfully created IAM role for AWS Lambda function!')
    time.sleep(10)
    add_permissions(role_name)
    return {
            'arn': create_iam_role['Role']['Arn'],
            'name': create_iam_role['Role']['RoleName']
        }  


def create_lambda(fcn_name, fcn_code, role_arn):
    print('Creating AWS Lambda function ...')
    new_fcn = lambda_client.create_function(
            FunctionName=fcn_name,
            Runtime='python3.8',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code=dict(ZipFile=fcn_code),
            Description=fcn_desc,
            Timeout=10,
            MemorySize=128,
            Publish=True
        )
    print('SUCCESS: Successfully created AWS Lambda function!')
    return new_fcn['FunctionArn']


def add_notif(bucket, prefix, lambda_fcn_arn):
    print('Initialising Amazon S3 Bucket client ...')
    bucket_notification = s3_resource.BucketNotification(bucket)
    print('SUCCESS: Successfully initilised Amazon S3 Bucket client!')
    print('Setting up notifications on Amazon S3 Bucket')
    setup_notif = bucket_notification.put(
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': lambda_fcn_arn,
                        'Events': ['s3:ObjectCreated:Put','s3:ObjectCreated:CompleteMultipartUpload'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'suffix',
                                        'Value': '.csv'
                                    },
                                    {
                                        'Name': 'prefix',
                                        'Value': prefix
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
    print('SUCCESS: Successfully added notifications to Amazon S3 Bucket!')
    
def create_s3_trigger(fcn_name,bucket,prefix, account_num, lambda_fcn_arn):
    bucket_arn = f"arn:aws:s3:::{bucket}"
    allow_s3(fcn_name,bucket_arn,account_num)
    add_notif(bucket, prefix, lambda_fcn_arn)


def describe_feature_group(fg_name):
    sagemaker_client=boto3.client('sagemaker')
    return sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)

def _get_offline_details(fg_name, sagemaker_client, s3_uri=None):
    _data_catalog_config = describe_feature_group(fg_name)['OfflineStoreConfig']['DataCatalogConfig']
    _table = _data_catalog_config['TableName']
    _database = _data_catalog_config['Database']

    if s3_uri is None:
        s3_uri = f's3://{sagemaker.Session().default_bucket()}/offline-store'
    _tmp_uri = f'{s3_uri}/query_results/'
    print(_tmp_uri)
    return _table, _database, _tmp_uri

def get_historical_record_count(fg_name, s3_uri=None):
    _table, _database, _tmp_uri = _get_offline_details(fg_name, s3_uri)
    print(_database)
    _query_string = f'SELECT COUNT(*) FROM "' +_table+ f'"'
    _tmp_df = _run_query(_query_string, _tmp_uri, _database, verbose=True)
    return _tmp_df.iat[0, 0]

def _run_query(query_string, tmp_uri, database, verbose=True):
    athena = boto3.client('athena')

    # submit the Athena query
    if verbose:
        print('Running query:\n ' + query_string)
    query_execution = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': tmp_uri}
    )
    
        # wait for the Athena query to complete
    query_execution_id = query_execution['QueryExecutionId']
    query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
    while (query_state != 'SUCCEEDED' and query_state != 'FAILED'):
        time.sleep(2)
        query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
    
    if query_state == 'FAILED':
        print(athena.get_query_execution(QueryExecutionId=query_execution_id))
        failure_reason = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['StateChangeReason']
        print(failure_reason)
        df = pd.DataFrame()
        return df
    else:
        ## TODO: fix this to avoid hardcoding prefix
        results_file_prefix = f'offline-store/query_results/{query_execution_id}.csv'

        # Prepare query results for training.
        filename = 'query_results.csv'
        results_bucket = (tmp_uri.split('//')[1]).split('/')[0]
        s3_client.download_file(results_bucket, results_file_prefix, filename)
        df = pd.read_csv('query_results.csv')
        os.remove('query_results.csv')
        
        s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix)
        s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix + '.metadata')
        return df