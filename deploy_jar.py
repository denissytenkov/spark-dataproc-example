#!/usr/bin/env python3.6

import argparse
import time
from googleapiclient.discovery import build
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials

def get_dataproc_client(key):
  """Builds a client to the dataproc API."""
  dataproc = build('dataproc', 'v1', credentials=key)
  return dataproc

def list_clusters(dataproc, project, region):
    result = dataproc.projects().regions().clusters().list(
        projectId=project,
        region=region).execute()
    return result

def list_jobs(dataproc, project, region):
    result = dataproc.projects().regions().jobs().list(
        projectId=project,
        region=region).execute()
    return result

def is_cluster_running(dataproc, project, region, cluster_name):
  result=list_clusters(dataproc,project,region)
  for cl in result['clusters']:
    if (cl['clusterName'] == cluster_name) and (cl['status']['state']=="RUNNING"):
      return True
  return False

def is_job_complete(dataproc, project, region, jobId):
    result=list_jobs(dataproc,project,region)
    for cl in result['jobs']:
        if (cl['reference']['jobId'] == jobId) and (cl['status']['state']=="DONE"):
            return True
    return False

def wait_until_created(dataproc, project, region, cluster_name):
    print("Waiting to start", end="",flush=True)
    while not is_cluster_running(dataproc,project,region,cluster_name):
       print(".", end="",flush=True)
       time.sleep(10)
    print("\nCluster {} is running".format(cluster_name))

def wait_until_complete(dataproc, project, region, jobId):
    print("Waiting for job to complete", end="",flush=True)
    while not is_job_complete(dataproc,project,region,jobId):
        print(".", end="",flush=True)
        time.sleep(10)
    print("\nJob {} is complete".format(jobId))

def create_cluster(dataproc, project, region, cluster_name):
    print("Starting DP cluster.")
    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'masterConfig': {
                'numInstances': 1,
                'machineTypeUri': 'n1-standard-4'
            },
    	    "softwareConfig": {
      		"properties": {
        		"dataproc:dataproc.allow.zero.workers": "true"
      		}
    	    }
        }
    }
    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()
    return result

def delete_cluster(dataproc, project, region, cluster_name):
    result = dataproc.projects().regions().clusters().delete(
        projectId=project,
        region=region,
        clusterName=cluster_name).execute()
    print("Deleted cluster {}".format(cluster_name))
    return result

def submit_job(dataproc, project, region, cluster_name, bucket_name, filename):
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
              'clusterName': cluster_name
            },
            'sparkJob': {
              'mainJarFileUri': 'gs://{}/{}'.format(bucket_name, filename)
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    return result['reference']['jobId']

def submit_job_and_wait(dataproc, project, region, cluster_name, bucket_name, filename):
    print("Submitting spark job to {}".format(cluster_name))
    job_id = submit_job(dataproc, project, region, cluster_name,bucket_name,filename)
    print("Job {} is submitted".format(job_id))
    wait_until_complete(dataproc, project, region, job_id)

def start_cluster_and_wait(dataproc, project, region, cluster_name):
    create_cluster(dataproc, project, region, cluster_name)
    wait_until_created(dataproc, project, region, cluster_name)

def upload_jar(key, backet, gs_location):
  print("Uploading file to GS.")
  storage_client = storage.Client.from_service_account_json(key)
  bucket = storage_client.get_bucket(backet)
  blob = bucket.blob(gs_location)

  blob.upload_from_filename("build/libs/scala-spark-all.jar")

def execute_command(commands, command, args):
    commands[command](args)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--project", help="GCP project to import data")
  parser.add_argument("--key", help="GCP key in json format")
  parser.add_argument("--bucket", help="Bucket to store jar, input and output")
  line_args, unkn_args = parser.parse_known_args()

  dataproc = get_dataproc_client(ServiceAccountCredentials.from_json_keyfile_name(line_args.key))

  commands = {
      "upload" : (lambda args : upload_jar(args.key, args.bucket, "executable/scala_spark.jar")),
      "start_cluster": (lambda args: start_cluster_and_wait(dataproc, args.project, "europe-west1", "spark-test")),
      "delete_cluster": (lambda args: delete_cluster(dataproc, args.project, "europe-west1", "spark-test")),
      "submit_job": (lambda args : submit_job_and_wait(dataproc, args.project, "europe-west1", "spark-test", args.bucket,"executable/scala_spark.jar"))
  }

  for com in unkn_args:
      execute_command(commands, com, line_args)
  #clusters = list_jobs(dataproc, args.project, "europe-west1")
  #clusters = list_clusters(dataproc, args.project, "europe-west1")
  #print(str(clusters))
