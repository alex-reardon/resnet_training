
import boto3


def main(filter_completed = True): 
    input_bucket = "loni-data-curated-20230501"
    input_prefix = 'ppmi_500_updated_cohort/curated/data/PPMI/'
    output_bucket = 'tempamr' # FIXME TEMP
    output_prefix = 'output_prefix/'  # FIXME TEMP
    modality = "T1w"
    
    keys = search_s3(input_bucket, input_prefix, modality)
    keys = reduce_keys(keys)   

    output_keys = search_s3(output_bucket, output_prefix, modality)
    output_keys = reduce_keys(output_keys)

    if filter_completed :
        input_parts = [key.split('PPMI/')[1] for key in keys]
        output_parts = [key.split('PPMI/')[1] for key in output_keys]
        keys = [input_prefix + key for key in input_parts if not any(part in key for part in output_parts)]


    for key in keys : 
        make_job(key, input_bucket, output_bucket, output_prefix)


def reduce_keys(keys):
    # Cuts of the filename at the end of the key so that we will only send
    # the names of the folders to the process
    prefixes = []
    for key in keys:
        prefix = '/'.join(key.split('/')[:-1]) + '/'
        prefixes.append(prefix)
    reduced = list(set(prefixes))
    return reduced

    
    
def search_s3(bucket, prefix, search_string):
    client = boto3.client('s3', region_name="us-east-1")
    paginator = client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = [] 
    for page in pages:
        contents = page['Contents']
        for c in contents:
            keys.append(c['Key'])
    if search_string:
        keys = [key for key in keys if search_string in key]
    return keys



def make_job(key, input_bucket, output_bucket, output_prefix):
    batch = boto3.client('batch', region_name='us-east-1')
    response = batch.submit_job(
        jobName='DTIProcessing',
        jobQueue='dti_processing',
        jobDefinition='arn:aws:batch:us-east-1:651875258113:job-definition/DTIProcessing:480',
        containerOverrides={
            "environment": [
                {
                    "name": "PROCESS_NAME",
                    "value": "DTI_Processing",
                },
                {
                    "name": "INPUT_BUCKET",
                    "value": input_bucket,
                }, 
                {
                    "name": "INPUT_PREFIX",
                    "value": key,
                },   
                {
                    "name": "OUTPUT_BUCKET",
                    "value": output_bucket,
                },
                {
                    "name": "OUTPUT_PREFIX",
                    "value": output_prefix,
                },
            ]
        },
        retryStrategy={
            "attempts": 3
        }
    )


    
if __name__ == "__main__":
    main()
