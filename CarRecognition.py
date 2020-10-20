import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs', endpoint_url='https://sqs.' + 'us-east-1' + '.amazonaws.com', region_name='us-east-1')

# User defined global variables
BUCKET_NAME = 'njit-cs-643'


def detect_labels(bucket, key, max_labels=10, min_confidence=90, region="us-east-1"):
	rekognition = boto3.client("rekognition", region)
	response = rekognition.detect_labels(
		Image={
			"S3Object": {
				"Bucket": bucket,
				"Name": key,
			}
		},
		MaxLabels=max_labels,
		MinConfidence=min_confidence,
	)
	return response['Labels']


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


def send_message(queue, message_body, message_group_id=None, message_attributes=None):
	"""
	Send a message to an Amazon SQS queue.

	Usage is shown in usage_demo at the end of this module.

	:param queue: The queue that receives the message.
	:param message_body: The body text of the message.
	:param message_group_id: This parameter applies only to FIFO (first-in-first-out) queues.
						The tag that specifies that a message belongs to a specific message group.
						Messages that belong to the same message group are processed in a FIFO manner
						(however, messages in different message groups might be processed out of order).
	:param message_attributes: Custom attributes of the message. These are key-value
								pairs that can be whatever you want.
	:return: The response from SQS that contains the assigned message ID.
	"""
	if not message_attributes:
		message_attributes = {}

	try:
		response = queue.send_message(
			MessageBody=message_body,
			MessageAttributes=message_attributes,
			MessageGroupId=message_group_id
		)
	except ClientError as error:
		logger.exception("Send message failed: %s", message_body)
		raise error
	else:
		return response


def main():
	# Setup Amazon S3 client
	s3 = boto3.resource('s3')

	# This is the queue where we will send our messages
	# Since we need to signify the end of the queue with '-1' index,
	# we need to use FIFO queue to main the order of the messages we are receiving.
	my_queue = get_queue("MehulVaghani_assignment01.fifo")

	all_files = s3.Bucket(BUCKET_NAME).objects.all()
	for file in all_files:
		for label in detect_labels('njit-cs-643', file.key):
			if label["Confidence"] > 90 and label["Name"] in ['Car']:
				# print(f'{label["Name"]} - {label["Confidence"]}')
				print(f"{format(label['Confidence'], '.2f')}% sure that {file.key} seems to be a car.")
				msg_attr = {
					'bucket_name': {
						'StringValue': file.bucket_name,
						'DataType': 'String'
					},

				}
				send_message_resp = send_message(my_queue, file.key, message_group_id='cs643', message_attributes=msg_attr)
				print(send_message_resp)
	send_message_resp = send_message(my_queue, '-1', message_group_id='cs643')


if __name__ == "__main__":
	main()
