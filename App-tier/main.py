import boto3
import os
import ast
import subprocess

aws_access_key_id = "" #Add your access ID here
aws_secret_access_key = "" #Add your secret key here 
region_name = "us-east-1"

#SQS
REQ_QUEUE = "1229481671-req-queue"
RESP_QUEUE = "1229481671-resp-queue"
SQS_REQUEST_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/975050025187/1229481671-req-queue"
SQS_RESPONSE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/975050025187/1229481671-resp-queue"

#S3
INPUT_BUCKET = "1229481671-in-bucket"
OUTPUT_BUCKET = "1229481671-out-bucket"

# Directories
INPUT_FOLDER = "temp_img"
OUTPUT_FOLDER = "uploaded_images"


session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
s3_client = session.client('s3')
sqs_client = session.client('sqs')
ec2_client = session.client('ec2', region_name='us-east-1')

#Image Classifier
def image_classify(image_path):
        """
        Performs image classification on the given image.
        Parameter: Path to the image (image_path)
        Returns the classification result
        """
        try:
            image_classifier_file_path = os.path.join(os.getcwd(),'face_recognition.py')
            image_path = image_path
            result = subprocess.run(['python3', image_classifier_file_path, image_path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            return result.stdout.strip().split('\n')[0]
        except Exception as e:
            print(f"Error in classifying the image: {e}")
            return None


#S3 Services

#Downloads a file from the specified S3 bucket.
def download_file(bucket_name, s3_object_key, download_file_path):
    try:
        s3_client.download_file(bucket_name, s3_object_key, download_file_path)
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False
    return True

#Upload an object to S3 with specified metadata.
def upload_object(bucket_name, s3_object_key, body, tagging, content_type, content_encoding):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_object_key,
            Body=body,
            Tagging=tagging,
            ContentType=content_type,
            ContentEncoding=content_encoding
        )
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False


#SQS Services

#Receives messages from the request SQS queue.
def receive_messages(queue_url, max_messages=1, wait_time=5):
    try:
        messages = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        ).get('Messages')
        return messages
    except Exception as e:
        print(f"Error receiving messages from SQS: {e}")
        return []

#Sends a message to the response SQS queue.
def send_message(queue_url, message_body):
    try:
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
        return True
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        return False

#Delete a message from the specified SQS queue.
def delete_message(queue_url, receipt_handle):
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return True
    except Exception as e:
        print(f"Error deleting message from SQS: {e}")
        return False
    
def stop_instance(instance_id):
    try:
        instance_info = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance_state = instance_info['Reservations'][0]['Instances'][0]['State']['Name']

        if instance_state == "running":
            ec2_client.stop_instances(InstanceIds=[instance_id])
        else:
            print(f"Instance {instance_id} is in {instance_state} state and cannot be started.")
    except Exception as e:
        print(f'Error trying to start instance: {e}')




#File Utils
    
def create_empty_file(file_name, file_path):
        """
        Create an empty file at the given path with the specified name.
        If the file already exists, just return the path to the file.
        """
        complete_file_path = os.path.join(file_path, file_name)
        if not os.path.exists(complete_file_path):
            with open(complete_file_path, 'w') as f:
                pass
        return complete_file_path

def process_output(input_image_path, classification_result, output_path):
    """
    Process the output image based on classification result.
    Rename and move the file.
    """
    try:
        input_image_path = input_image_path.split('/')[-1]
        output_file_name = f'{input_image_path.split(".")[0]}.txt'
        output_file_path = os.path.join(output_path, output_file_name)
        with open(output_file_path,'w') as file:
            file.write(input_image_path.split('.')[0] +":"+ classification_result)
        return {
            'message': True,
            'output_file_name': output_file_name,
            'output_file_path': output_file_path
        }
    except Exception as e:
        print(e)
        return {
            'message': False,
            'output_file_name': ""
        }
    
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





while True:
    #Continuously poll for messages 
    print("Polling for messages")
    messages = receive_messages(queue_url=SQS_REQUEST_QUEUE_URL)
    # print(messages)

    # [{'MessageId': 'd3e81916-e59a-4b71-aab8-a0261a4850f3', 'ReceiptHandle': 'AQEBHYeKxfrbFofNOfUh8/RG+iChZ2iCIaGxl8Ogr8YhjYpMLkGDQN+WFbrq8xCWmY/HDV+nTC9z2h+qF9rc4XYyxNtvX5Wy4K1+uNY9fUg2+dozm62rEDcF5q7P1sYXA/VPXmlxNRlSY9z8fXX04hZClCbr07vz7H1KYHq5PR9R5D83ZQyv5ZERUl/HRUIqPeuDMrm7rRnPdd+4khfCVJoMc7JY66LTkdAXTXsU+3jKRahze65bHVo0QwkOeXaVEsXW7cGemLHm0K0q9QtvzLyKdG5WzFq6l6oA5yDcpj3PU2b2kAKZGJ4+ik7WK6YtTH6Nu0WtCMy2Zu/aIc/DhC0aATtRtQOnqWXYuJhLro2O0vusHmyAjyVPPy87uJvXpeI9dMmP3NZzOpmvZv1k/Kn65w==', 'MD5OfBody': 'f26e63747927ece1efeb857b0c17a29b', 'Body': "{'s3_object_key': 'test_019.jpg', 'file_name': 'test_019.jpg', 'bucket_name': '1229481671-in-bucket', 'uploaded': True}"}]

    '''
    If a message is present in the Request Queue
        Download that image and run the image classification file on it
        Upload the classification result along with the image name to the output bucket
        Also send a message to the response queue, then delete the message
        Remove the file from the system
    '''
    if messages:
        for message in messages:
            print(message)
            metadata = ast.literal_eval(message['Body'])
            print(metadata)
            #message_id = message['MessageId']
            #print(message_id)
            downlaod_bucket_name = metadata['bucket_name']
            download_file_path = create_empty_file(file_name=metadata['s3_object_key'], file_path=os.path.join(os.getcwd(),INPUT_FOLDER))
            print(download_file_path)
            if not download_file(bucket_name=downlaod_bucket_name, s3_object_key=metadata['s3_object_key'], download_file_path=download_file_path):
                print('In')
                continue
            print("Continuing")
            img_classifier_res = image_classify(download_file_path)
            print(img_classifier_res)
            if img_classifier_res:
                create_output = process_output(input_image_path=download_file_path, classification_result=img_classifier_res, output_path=OUTPUT_FOLDER)
                if create_output['message']:
                    output = {
                        "uuid": metadata['uuid'], 
                        "image_file":download_file_path,
                        "classification" :  img_classifier_res
                    }
                    # if metadata['uuid'] == output['uuid']:
                    upload_to_output_bucket = upload_object(bucket_name=OUTPUT_BUCKET, s3_object_key=create_output['output_file_name'].split('.')[0],body=str(output), tagging = f"uuid={metadata['uuid']}", content_type='text/plain',content_encoding='utf-8')
                    if upload_to_output_bucket:
                        send_message_to_sqs = send_message(queue_url=SQS_RESPONSE_QUEUE_URL, message_body=str(output))
                        if send_message_to_sqs:
                            delete_message(queue_url=SQS_REQUEST_QUEUE_URL, receipt_handle=message['ReceiptHandle'])
                            os.remove(download_file_path)
                            os.remove(create_output['output_file_path'])
                            
                            #Checks for queue length and self-terminates if the queue length is 0 (i.e no more pending requests to be handled)
                            request_queue_length = int(
                            sqs_client.get_queue_attributes(QueueUrl=REQ_QUEUE, AttributeNames=['ApproximateNumberOfMessages']).get(
                                "Attributes").get("ApproximateNumberOfMessages"))
                            request_queue_length_in_flight = int(
                                sqs_client.get_queue_attributes(QueueUrl=SQS_REQUEST_QUEUE_URL, AttributeNames=['ApproximateNumberOfMessagesNotVisible']).get(
                                    "Attributes").get("ApproximateNumberOfMessagesNotVisible"))
                            queue_length = request_queue_length + request_queue_length_in_flight
                            ec2_metadata = session.client('ec2', region_name='us-east-1')
                            response = ec2_client.describe_instances()
                            instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
                            if queue_length == 0:
                                stop_instance(instance_id=instance_id)

