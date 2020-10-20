import time
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs', endpoint_url='https://sqs.' + 'us-east-1' + '.amazonaws.com', region_name='us-east-1')
output_file = 'results.txt'
time_today = (time.strftime("%Y%m%d-%H_%M_%S"))

# User defined global variables
BUCKET_NAME = 'njit-cs-643'


def get_object(bucket, object_key):
	"""
	Gets an object from a bucket.

	Usage is shown in usage_demo at the end of this module.

	:param bucket: The bucket that contains the object.
	:param object_key: The key of the object to retrieve.
	:return: The object data in bytes.
	"""
	try:
		body = bucket.Object(object_key).get()['Body'].read()
		logger.info("Got object '%s' from bucket '%s'.", object_key, bucket.name)
	except ClientError:
		logger.exception(("Couldn't get object '%s' from bucket '%s'.", object_key, bucket.name))
		raise
	else:
		return body


def detect_text(photo, bucket, region="us-east-1"):
	client = boto3.client('rekognition', region)

	response = client.detect_text(Image={'S3Object': {'Bucket': bucket, 'Name': photo}})
	# texts = [RekognitionText(text) for text in response['TextDetections']]
	# logger.info("Found %s texts in %s.", len(texts), self.image_name)
	text_detections = response['TextDetections']
	print('Detected text\n----------')
	for text in text_detections:
		print('Detected text:' + text['DetectedText'])
		print('Confidence: ' + "{:.2f}".format(text['Confidence']) + "%")
		print('Id: {}'.format(text['Id']))
		if 'ParentId' in text:
			print('Parent Id: {}'.format(text['ParentId']))
		print('Type:' + text['Type'])
		print()
	return text_detections


def get_queue(name):
	"""
	Gets an SQS queue by name.

	:param name: The name that was used to create the queue.
	:return: A Queue object.
	"""
	try:
		queue = sqs.get_queue_by_name(QueueName=name)
		logger.info("Got queue '%s' with URL=%s", name, queue.url)
	except ClientError as error:
		logger.exception("Couldn't get queue named %s.", name)
		raise error
	else:
		return queue


def receive_messages(queue, max_number, wait_time):
	"""
	Receive a batch of messages in a single request from an SQS queue.

	Usage is shown in usage_demo at the end of this module.

	:param queue: The queue from which to receive messages.
	:param max_number: The maximum number of messages to receive. The actual number
						of messages received might be less.
	:param wait_time: The maximum time to wait (in seconds) before returning. When
						this number is greater than zero, long polling is used. This
						can result in reduced costs and fewer false empty responses.
	:return: The list of Message objects received. These each contain the body
				of the message and metadata and custom attributes.
	"""
	try:
		messages = queue.receive_messages(
			MessageAttributeNames=['All'],
			MaxNumberOfMessages=max_number,
			WaitTimeSeconds=wait_time
		)
		for msg in messages:
			logger.info("Received message: %s: %s", msg.message_id, msg.body)
	except ClientError as error:
		logger.exception("Couldn't receive messages from queue: %s", queue)
		raise error
	else:
		return messages


def delete_message(message):
	"""
	Delete a message from a queue. Clients must delete messages after they
	are received and processed to remove them from the queue.

	Usage is shown in usage_demo at the end of this module.

	:param message: The message to delete. The message's queue URL is contained in
					the message's metadata.
	:return: None
	"""
	try:
		message.delete()
		logger.info("Deleted message: %s", message.message_id)
	except ClientError as error:
		logger.exception("Couldn't delete message: %s", message.message_id)
		raise error


def main():

	# Setup Amazon S3 client
	s3 = boto3.resource('s3')

	# This is the queue where we will send our messages
	# Since we need to signify the end of the queue with '-1' index,
	# we need to use FIFO queue to main the order of the messages we are receiving.
	my_queue = get_queue("MehulVaghani_assignment01.fifo")
	received_messages_list = receive_messages(my_queue, 10, 5)

	with open(output_file, "a") as myfile:
		myfile.write(f'---------------------------------------\n')
		myfile.write(f'------ Ran on: {time_today}------\n')
		myfile.write(f'---------------------------------------\n')

	for msg in range(len(received_messages_list)):

		# Message with value -1 is to indicate to stop processing,
		# therefore there is no need to process it.
		if not received_messages_list[msg].body == '-1':
			message_url = received_messages_list[msg].queue_url
			bucket_name = received_messages_list[msg].message_attributes['bucket_name']['StringValue']
			object_index = received_messages_list[msg].body

			labels = detect_text(photo=object_index, bucket=bucket_name)
			# print(labels)

			if len(labels) > 0:
				# print(labels)
				for l in range(len(labels)):
					if labels[l]['Type'] == 'LINE' and labels[l]['Confidence'] > 90:
						with open(output_file, "a") as myfile:
							myfile.write(f'Image Index: {object_index}\n')
							myfile.write(f'Text Found:  {labels[l]["DetectedText"]}\n')
							myfile.write(f'#######################\n')

		try:
			delete_message(message=received_messages_list[msg].queue_url)
		except AttributeError as error:
			logger.info(f'No attribute delete for the object')
	with open(output_file, "a") as myfile:
		myfile.write('\n\n')


if __name__ == "__main__":
	main()
