import math
import boto3
import time
import base64
from datetime import timedelta

aws_access_key_id = "" #Add your access ID here
aws_secret_access_key = "" #Add your secret key here 
region_name = "us-east-1"


INPUT_BUCKET = "1229481671-in-bucket"
OUTPUT_BUCKET = "1229481671-out-bucket"
REQ_QUEUE = "1229481671-req-queue"
RESP_QUEUE = "1229481671-resp-queue"
SQS_REQUEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/975050025187/1229481671-req-queue"
SQS_RESPONSE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/975050025187/1229481671-resp-queue"

WEB_TIER = "i-0518663818554439f"
APP_TIER = 'i-0ec3efd4f0c7d5230'
APP_TIER_AMI = 'ami-080e1f13689e07408'

session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
sqs_client = session.client('sqs', region_name="us-east-1")
ec2_client = session.client('ec2', region_name="us-east-1")
s3_client = session.client('s3', region_name="us-east-1")

def get_running_instances():
    try:
        response = ec2_client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running','pending']}])
        instances = [instance['InstanceId'] for reservation in response['Reservations'] for instance in reservation['Instances']]
        return instances
    except Exception as e:
        print(f'Error getting the running instances: {e}')

def get_queue_length(queue_url):
    try:
        attributes = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessages'])
        return int(attributes['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        print(f"Error getting queue length for {queue_url}: {e}")
        return None

def in_flight_messages_length(queue_url):
    try:
        attributes = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessagesNotVisible'])
        return int(attributes['Attributes']['ApproximateNumberOfMessagesNotVisible'])
    except Exception as e:
        print(f"Error getting queue length for {queue_url}: {e}")
        return None

def get_stopped_instances():
    try:
        response = ec2_client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped','stopping']}])
        instances = [instance['InstanceId'] for reservation in response['Reservations'] for instance in reservation['Instances']]
        return instances
    except Exception as e:
        print(f'Error getting the stopped instances: {e}')


def start_instance(instance_id):
    try:
        instance_info = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance_state = instance_info['Reservations'][0]['Instances'][0]['State']['Name']

        if instance_state == "stopped":
            ec2_client.start_instances(InstanceIds=[instance_id])
        else:
            print(f"Instance {instance_id} is in {instance_state} state and cannot be started.")
    except Exception as e:
        print(f'Error trying to start instance: {e}')


def start_multiple_instances(instance_ids):
    try:
        for instance_id in instance_ids:
            start_instance(instance_id)
    except Exception as e:
        print(f'Error starting multiple instances: {e}')

def stop_multiple_instances(instance_ids):
    try:
        running_instances = [i for i in instance_ids if ec2_client.describe_instances(InstanceIds=[i])['Reservations'][0]['Instances'][0]['State']['Name'] == 'running']
        if running_instances:
            ec2_client.stop_instances(InstanceIds=running_instances)
    except Exception as e:
        print(f'Error trying to stop multiple instances: {e}')

def get_highest_instance_number():
    try:
        all_instances = get_running_instances() + get_stopped_instances()
        instance_numbers = []

        for instance_id in all_instances:
            instance_tags = ec2_client.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0].get('Tags', [])
            for tag in instance_tags:
                if tag['Key'] == 'Name' and 'app-tier-instance' in tag['Value']:
                    try:
                        instance_numbers.append(int(tag['Value'].split('-')[-1]))
                    except ValueError:
                        pass

        if instance_numbers:
            return max(instance_numbers)
        else:
            return 1
    except Exception as e:
        print(f'Error trying to get the highest instance number: {e}')

def auto_scale_instances():
    try:
        request_queue_length = int(
            sqs_client.get_queue_attributes(QueueUrl=REQ_QUEUE, AttributeNames=['ApproximateNumberOfMessages']).get(
                "Attributes").get("ApproximateNumberOfMessages"))
        request_queue_length_in_flight = int(
            sqs_client.get_queue_attributes(QueueUrl=SQS_REQUEST_QUEUE_URL, AttributeNames=['ApproximateNumberOfMessagesNotVisible']).get(
                "Attributes").get("ApproximateNumberOfMessagesNotVisible"))
        queue_length = request_queue_length + request_queue_length_in_flight

        print("Request queue length:", queue_length)

        running_instances = get_running_instances()
        print(running_instances)
        stopped_instances = get_stopped_instances()
        print(stopped_instances)

        if(WEB_TIER in running_instances):
            running_instances.remove(WEB_TIER)

        instances_required = 0
        if queue_length > 12:
            instances_required = 11
        else:
            instances_required = queue_length

        if len(running_instances) < instances_required:
            instances_needed = instances_required - len(running_instances)
            start_multiple_instances(stopped_instances[:instances_needed])
        running_instances = get_running_instances()
        print(running_instances)
        stopped_instances = get_stopped_instances()
        print(stopped_instances)

    except Exception as e:
        print(f'Error trying to autoscale: {e}')



if __name__ == "__main__":
    while True:
        print("Running Autoscale script")
        auto_scale_instances()
        time.sleep(12)
