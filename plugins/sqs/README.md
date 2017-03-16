#[Description] (SQS plugin allows to push message using ECL code)
   * First you have to put your aws accessKey in the credential file into .aws folder. This folder is located in HPCC's home dir: "~/hpcc/.aws/" if it doesn't exist, you should create it.

#[Example]
 * import sqs;
	publisher:=sqs.SQSPublisher('QueueName');
	publisher.PublishMessage('Put your message here');







