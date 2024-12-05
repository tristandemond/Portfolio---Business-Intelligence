from argparse import ArgumentParser
from decimal import Decimal
import json
from os import path
from pathlib import Path
from subprocess import run

import boto3

from helpers.aws import aws

'''
Steps the script will take when a merge happens
1. Reads the current dynamo entries in Prod
2. Compares against the current state of the input json entries and finds the diff
3. For new records adds insert_dt and for updated_records adds updated_dt
4. Uploads the changes to Prod Dynam DB Table
5. Runs the glue job only for the changed records
6. Kicks off crawler
'''


FIVETRAN_SCRIPT = 'fivetran.py'
REGION_NAME = 'us-west-2'

p = Path(__file__)
errors = []
dir_abs = p.parent.absolute()

## Pass arguments for Github Action to pass file path where changes occured , access key and secret key
parser = ArgumentParser()
parser.add_argument('file_path')
parser.add_argument('access_key_id')
parser.add_argument('secret_access_key')
args = parser.parse_args()
path_parts = args.file_path.split(path.sep)

# Find the file that got changed after input. Since the folder is named same as dynamo
directory_index = path_parts.index('input')

# Dynamo DB table name is just after the input
dynamo_table_name = path_parts[directory_index + 1]

def create_connector(schema, cwd=None):
    args_ = ['python3', FIVETRAN_SCRIPT, '-t', 'prod', schema]
    run_subprocess(args_, cwd=cwd)

def update_connector(schema, cwd=None):
    args_ = ['python3', FIVETRAN_SCRIPT, '-i', '-t', 'prod', schema]
    connector_id = run_subprocess(args_, cwd=cwd).strip()
    args_ = ['python3', FIVETRAN_SCRIPT, '-u', '-t', 'prod', connector_id]
    return run_subprocess(args_, cwd=cwd)

def run_subprocess(args_, capture_output=True, cwd=None):
    result = run(args_, capture_output=capture_output, cwd=cwd)
    if result.returncode:
        raise Exception(result.stderr.decode())
    else:
        return result.stdout.decode()

# 1. Reads the current dynamo entries in Prod
if __name__ == '__main__':
    logger = aws.get_logger('Dynamo DB Entries')
    session = boto3.Session(
        aws_access_key_id=args.access_key_id,
        aws_secret_access_key=args.secret_access_key,
        region_name=REGION_NAME
    )

    # 1. Reads the current dynamo entries in Prod
    prod_ddb_items = aws.read_item_ddb_tables(dynamo_table_name, REGION_NAME)
    prod_ddb_entries = [json.loads(json.dumps(item)) for item in prod_ddb_items]
    dynamodb_keys = {
        json.dumps(entry, sort_keys=True) for entry in prod_ddb_entries
    }

    # 2. Compares against the current state of the input json entries and finds the diff
    with open(args.file_path, 'r') as my_file:
        local_entries = json.load(my_file, parse_float=Decimal)
    local_keys = {
        json.dumps(entry, sort_keys=True) for entry in local_entries
    }

    # Convert the DynamoDB items to JSON objects
    # Find the difference between the two lists of keys
    diff_keys = local_keys - dynamodb_keys

    # Convert the difference back to JSON objects
    diff_items = [json.loads(key) for key in diff_keys]
    logger.info(f'Newly Added Entries to be added to Dynamo DB Config {diff_items} and {len(diff_items)}')

    # 3. Update dynamo for all entries
    if len(diff_items):
        if dynamo_table_name in ('client_os_fivetran_connector', 'client_os_fivetran_connector_config'):
            dynamodb_ids = {i['id'] for i in prod_ddb_items}
            cwd = path.dirname(path.realpath(__file__))
        for objects in diff_items:
            try:
                logger.info(f'Adding dynamo entry {objects}')
                aws.put_item_ddb_table(
                    dynamo_table_name, REGION_NAME, logger, **objects
                )

                # 4. Run Glue or Fivetran script for changed entries
                if objects.get('is_active') and dynamo_table_name == 'client_os_spreadsheet_ingestion':
                    job_parameter = {'--sheet_link': objects.get('sheet_link')}
                    logger.info(f'Running glue job for {job_parameter}')
                    aws.glue_start_job(
                        job_name='KPI-GLUE-PARAMS-JOB-os_client_gsheet',
                        region_name=REGION_NAME,
                        logger=logger,
                        job_parameter=job_parameter,
                    )
                elif objects.get('active')=='True' and dynamo_table_name == 'client_os_ingestion_data_object':
                    job_parameter = {'--data_object_id': objects.get('id'),'--refresh_type':'full'}
                    logger.info(f'Running glue job for {job_parameter}')
                    aws.glue_start_job(
                        job_name='KPI-GLUE-PARAMS-JOB-mobius_data_ingestion',
                        region_name=REGION_NAME,
                        logger=logger,
                        job_parameter=job_parameter,
                    )
                elif dynamo_table_name == 'client_os_fivetran_connector':
                    if objects['id'] in dynamodb_ids:
                        print(update_connector(objects['id'], cwd=cwd))
                    else:
                        print(create_connector(objects['id'], cwd=cwd))
                elif dynamo_table_name == 'client_os_fivetran_connector_config':
                    if objects['id'] in dynamodb_ids:
                        connectors = [
                            c for c in aws.read_item_ddb_tables(
                                'client_os_fivetran_connector',
                                REGION_NAME
                            ) if c['service'] == objects['id']
                        ]
                        for connector in connectors:
                            print(update_connector(connector['id'], cwd=cwd))
            except Exception as e:
                error_msg = f'{objects.get("sheet_link")}: {str(e)}'
                errors.append(error_msg)
                continue

        if len(errors) > 0:
            print('The following errors occurred during processing:')
            for error in errors:
                print(error)
        else:
            # 5 Run Crawler
            if dynamo_table_name =='client_os_spreadsheet_ingestion':
                aws.trigger_crawler(
                    'client_os_source_files',
                    REGION_NAME,
                    logger
                )
    else:
        print('No changes detected to be merged')