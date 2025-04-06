# from fastapi import FastAPI, File, UploadFile, HTTPException
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
# from fastapi.responses import PlainTextResponse
import csv
import os
import boto3
import ast
import datetime
import uuid
from threading import Lock

unprocessed_messages = {}
lock = Lock()

# app = FastAPI()
app = Flask(__name__)
CORS(app)

aws_access_key_id = "" #Add your access ID here
aws_secret_access_key = "" #Add your secret key here 
region_name = "us-east-1"


INPUT_BUCKET = "1229481671-in-bucket"
OUTPUT_BUCKET = "1229481671-out-bucket"
REQ_QUEUE = "1229481671-req-queue"
RESP_QUEUE = "1229481671-resp-queue"


session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
s3_client = session.client('s3')
sqs_client = session.client('sqs')

# This is the folder we create to store the uploaded images
upload_folder_path = "uploaded_images"

# This function gets the contents of the csv file in the form of a list of dictionaries
def csv_extract(csv_file):
    data = []
    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data
    

def upload_to_s3(file_name, bucket, extra_args, object_name=None):
    '''
    Uploads the file into the s3 bucket
    '''
    if object_name is None:
        object_name = file_name.split('/')[-1]

    try:
        s3_client.upload_file(file_name, bucket, object_name, ExtraArgs =extra_args)
        return {
            's3_object_key': object_name,
            'uuid': extra_args['Metadata']['uuid'],
            'file_name': os.path.basename(file_name),
            'bucket_name': bucket,
            'uploaded': True
            }
    
    except Exception as e:
        return {
            'error': str(e),
            'uploaded': False
        }
    
def is_allowed_file_format(filename, allowed_extensions={'png', 'jpg', 'jpeg'}):
    """Check if the given filename has an allowed extension."""
    try:
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions
    except Exception as e:
        print(f"Error checking file format for {filename}: {e}")
        return False

def create_directory(directory_path):
    """Create the specified directory if it does not exist."""
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
    except Exception as e:
        print(f"Error creating directory {directory_path}: {e}")
    
def send_message(queue_url, message_body):
    """Send a message to the given SQS queue."""
    try:
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
        return {
            'status': True,
            'message_id': response['MessageId']
        }
    except Exception as e:
        return {
            'status': False,
            'message': str(e)
        }

def receive_messages(queue_url, max_messages=2, wait_time=20):
    """Receive messages from the given SQS queue."""
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
        return response.get('Messages')
    
    except Exception as e:
        print(f'Error receiving messages: {e}')
        return None

def delete_message(queue_url, receipt_handle):
    """Delete a message from the given SQS queue using its receipt handle."""
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return True
    except Exception as e:
        print(f'Error deleting message: {e}')
        return False
        
@app.route('/', methods=['GET', 'POST'])
def upload_image():
    if request.method == 'GET':
        return jsonify({
            'message': 'Hello from the web-tier'
        })
    if request.method == 'POST':
        
        file = request.files.get('inputFile')
        if not file or file.filename == '':
            return jsonify({'error': 'No file provided or file selected'})

        if not is_allowed_file_format(file.filename):
            return jsonify({'error': 'Invalid file format'})
        
        # new_file_name = f"{unique_id}_{file.filename}"
        unique_id = file.filename
        file_path = os.path.join(upload_folder_path, file.filename)
        file.save(file_path)
        tag_string = f"uuid={unique_id}"
        upload_res = upload_to_s3(file_name=file_path, bucket=INPUT_BUCKET, extra_args={
            'Metadata': {
                'uuid': unique_id
            },
            'Tagging': tag_string
        })

        if upload_res['uploaded']:
            req_queue = send_message(queue_url=REQ_QUEUE, message_body=str(upload_res))
            if req_queue['status']:
                response_timeout = datetime.datetime.now() + datetime.timedelta(seconds=100000000)
                try:
                    while datetime.datetime.now() < response_timeout:
                        messages = receive_messages(queue_url=RESP_QUEUE)

                        if messages:
                            msg = messages[0]
                            msg_body = ast.literal_eval(msg['Body'])
                            msg_uuid = msg_body.get('uuid', None)

                            unprocessed_messages[msg_uuid] = {"body": msg_body, "receipt_handle": msg['ReceiptHandle']}
                            if msg_uuid == unique_id:
                                delete_message(queue_url=RESP_QUEUE, receipt_handle=msg['ReceiptHandle'])
                                resp = make_response(msg_body['image_file'].split('/')[-1].split('.')[0] + ":" + msg_body['classification'], 200)
                                resp.mimetype = 'text/plain'
                                return resp
                            
                            if unique_id in unprocessed_messages:
                                stored_msg = unprocessed_messages[unique_id]["body"]
                                receipt_handle = unprocessed_messages[unique_id]["receipt_handle"]
                                delete_message(queue_url=RESP_QUEUE, receipt_handle=receipt_handle)
                                resp = make_response(stored_msg['image_file'].split('/')[-1].split('.')[0] + ":" + stored_msg['classification'], 200)
                                resp.mimetype = 'text/plain'
                                del unprocessed_messages[unique_id]
                                return resp
                
                except Exception as e:
                    return jsonify({str(e)})
                
if __name__ == '__main__':
    create_directory(os.path.join(os.getcwd(),upload_folder_path))
    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
