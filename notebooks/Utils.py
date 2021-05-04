#Import libraries
import boto3
import json
import botocore
import time
from random import randint
from functools import wraps
import logging

#Initialise AWS Clients
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')
s3_client = boto3.resource('s3')

def create_lambda_fcn(flow_bucket, flow_key, pipeline_name):
    
    #Set variables
    print('Gathering variables ...')
    
    flow_bucket = flow_bucket
    flow_key = flow_key
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
        execution_display = f"Execution for {data_key}"


        try:
            #Get .flow file from Amazon S3
            get_object = s3.Object(flow_bucket,flow_key)
            get_flow = get_object.get()

            try:
                #Read, update and save the .flow file
                flow_content = json.loads(get_flow['Body'].read())
                flow_content['nodes'][0]['parameters']['dataset_definition']['name'] = data_key
                flow_content['nodes'][0]['parameters']['dataset_definition']['s3ExecutionContext']['s3Uri'] = f"s3://{data_bucket}/{data_key}"
                new_flow_key = flow_key.replace('.flow', '-' + data_key.replace('.csv','') + '.flow')
                new_flow_uri = f"s3//{flow_bucket}/{new_flow_key}"
                put_object = s3.Object(flow_bucket,new_flow_key)
                put_flow = put_object.put(Body=json.dumps(flow_content))

                try:
                    #Start the pipeline execution
                    start_pipeline = sm.start_pipeline_execution(
                        PipelineName=pipeline_name,
                        PipelineExecutionDisplayName=pipeline_display,
                        PipelineParameters=[
                            {
                                'Name': 'InputFlow',
                                'Value': new_flow_uri
                            },
                        ],
                        PipelineExecutionDescription=data_key
                        )
                    print(start_pipeline)
                except Exception as e:
                    print(e)
                    raise

            except Exception as e:
                print(e)
                raise

        except Exception as e:
            print(e)
            raise

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

#Define Python Decorator for retries
def try_except_retry(count=3, multiplier=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _count = count
            _seconds = randint(5,10)
            while _count >= 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning("{}, Trying again in {} seconds".format(e, _seconds))
                    time.sleep(_seconds)
                    _count -= 1
                    _seconds *= multiplier
                    if _count == 0:
                        logging.error("Retry attempts failed, raising the exception.")
                        raise
            return func(*args, **kwargs)
        return wrapper
    return decorator

#Define function to allow Amazon S3 to trigger AWS Lambda
@try_except_retry()
def allow_s3(fcn_name,bucket_arn,account_num):
    print('Adding permissions to Amazon S3 ...')
    try:
        response = lambda_client.add_permission(
            FunctionName=fcn_name,
            StatementId=f"S3-Trigger-Lambda-{int(time.time())}",
            Action='lambda:InvokeFunction',
            Principal= 's3.amazonaws.com',
            SourceArn=bucket_arn,
            SourceAccount=account_num
        )
        print('SUCCESS: Successfully added permissions to Amazon S3!')
    except Exception as e:
        print(e)
        raise

#Define Function to create IAM role
@try_except_retry()
def create_role(role_name, iam_desc):
    print('Creating an IAM role for AWS Lambda function ...')
    try:
        create_iam_role = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(iam_trust_policy),
        Description=iam_desc
        )
        print('SUCCESS: Successfully created IAM role for AWS Lambda function!')
        return {
            'arn': create_iam_role['Role']['Arn'],
            'name': create_iam_role['Role']['RoleName']
        }  
    except Exception as e:
        print(e)
        raise

@try_except_retry()
def add_permissions(name):
    print("Adding permissions to AWS Lambda function's IAM role ...")
    try:
        add_execution_role = iam_client.attach_role_policy(
            RoleName=name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        try:
            add_execution_role = iam_client.attach_role_policy(
                RoleName=name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonSageMakerFullAccess'
            )
            print("SUCCESS: Successfully added permissions AWS Lambda function's IAM role!")
        except Exception as e:
            print(e)
            raise
    except Exception as e:
        print(e)
        raise

@try_except_retry()
def create_lambda(fcn_name, fcn_desc, fcn_code, role_arn):
    print('Creating AWS Lambda function ...')
    try:
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
    except Exception as e:
        print(e)
        raise

@try_except_retry()
def add_notif(bucket, lambda_fcn_arn):
    print('Initialising Amazon S3 Bucket client ...')
    bucket_notification = s3_client.BucketNotification(bucket)
    print('SUCCESS: Successfully initilised Amazon S3 Bucket client!')
    print('Setting up notifications on Amazon S3 Bucket')
    try:
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
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
        print('SUCCESS: Successfully added notifications to Amazon S3 Bucket!')
    except Exception as e:
        print(e)
        raise