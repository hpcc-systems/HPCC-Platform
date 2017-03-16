# Description 
   * First you have to put your aws accessKey in the credential file into .aws folder. This folder is located into home dir of hpcc process. "~/hpcc/.aws/" if it doesn't exist, you should create it.

##[Example]
 * import sqs;
	publisher:=sqs.SQSPublisher('QueueName');
	publisher.PublishMessage('Put your message here');







