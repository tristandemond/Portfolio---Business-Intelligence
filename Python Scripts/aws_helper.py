import boto3
import logging
import sys
import time



def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger



def put_item_ddb_table(table_name,region_name,logger,**kwargs):
    data = {**kwargs}
    ddb = boto3.resource('dynamodb', region_name=region_name)
    table = ddb.Table(table_name)
    response= table.put_item(Item=data)
    logger.info(f'Dynamo Entries {data}')
    return response


def read_item_ddb_tables(table_name,region_name):
    dynamodb = boto3.resource('dynamodb',region_name=region_name)
    table = dynamodb.Table(table_name)
    response = table.scan()

    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
        
    return(items)

def glue_start_job(job_name,region_name,logger,job_parameter):

    session = boto3.Session(region_name=region_name)
    glue = session.client('glue')

    # Define the parameters for the Glue job
    job_name = job_name
    job_params = job_parameter

# Kick off the Glue job
    response = glue.start_job_run(JobName=job_name,Arguments=job_params)

# Wait for the Glue job to complete
    job_run_id = response['JobRunId']
    while True:
        response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        if status == 'SUCCEEDED':
            logger.info("Glue Job Completed Sucessfully")
            break
        elif status == 'FAILED':
            raise RuntimeError('Glue job failed')
        else:
            time.sleep(10)

# Kick off the Glue crawler

def trigger_crawler(crawler_name,region_name,logger,):
    session = boto3.Session(region_name=region_name)
    glue = session.client('glue')
    crawler_name = crawler_name
    response = glue.start_crawler(Name=crawler_name)

# Wait for the crawler to complete
    while True:
        response = glue.get_crawler(Name=crawler_name)
        status = response['Crawler']['State']
        if status == 'READY':
            logger.info("The Crawler completed sucessfully")
            break
        elif status == 'FAILED':

            raise RuntimeError('Glue crawler failed')
            
        else:
            time.sleep(10)

    # If we made it here, both the job and the crawler succeeded
    logger.info('Glue job and crawler succeeded')