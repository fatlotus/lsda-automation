#!/usr/bin/env python
#
# A persistent worker daemon to copy output from AMQP into S3.
# 
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 12 January 2013

import boto.s3
import boto.ses
from boto.s3.key import Key
import gevent
import pika
import functools
import shutil
import yaml
import tempfile

# Too happy?
EMAIL_TEMPLATE = """\
Hello there!

One of your compute jobs has just finished; the results are located at
the page below.

http://ml-submissions.s3-website-us-east-1.amazonaws.com/?{task_id}

Best of luck,

-J
"""

def process_log_line(ses, bucket, target_directory, temp_directory, message):
   """
   Processes a log line from AMQP.
   """
   
   # Determine where to write the output for this task.
   task_id = message["task_id"]
   stdout_path = os.path.join(temp_directory, task_id + ".txt")
   
   # If the stream has closed,
   if message["type"] == "done":
      
      # write the results to S3,
      key = bucket.new_key(os.path.join(target_directory, task_id))
      key.set_contents_from_filename(stdout_path)
      
      # and begin preparing an email for the user,
      branch_name = message["branch_name"] 
      match = re.match(r"submissions/([^/]+)/submit$", branch_name)
      cnetid = match.group(1) if match else None
      
      # if possible;
      if cnetid:
         ses.send_email(
            "\"Cylon Jeremy\" <jarcher@uchicago.edu>",
            "AUTO: Completed Run",
            EMAIL_TEMPLATE.format(**locals())
         )
      
   else:
      
      # otherwise, append the line to the local logging file.
      with open(stdout_path, "a") as fp:
         fp.write("{level:5} {message}\n".format(message))

def main():
   # Read configuration.
   options = yaml.load(open("config.yaml"))
   
   # Choose a temporary working directory.
   temp_directory = tempfile.mkdtemp()
   
   try:
      
      # Connect to s3.
      email_connection = boto.ses.connect_to_region("us-east-1")
      storage_connection = boto.connect_s3()
      
      target_bucket = storage_connection.get_bucket(
                        options["submit_bucket_name"])
      target_directory = options["submit_target_directory"]
      
      # Connect to AMQP.
      parameters = pika.ConnectionParameters(options["amqp"])
      
      connection = pika.BlockingConnection(parameters)
      channel = connection.channel()
      
      # Subscribe to the worker queue.
      queue_name = channel.queue_declare(exclusive=True).method.queue
      
      channel.queue_bind(
         exchange = "lsda_logs",
         queue = queue_name,
         routing_key = "stderr.*"
      )
      
      # Begin consuming all remaining AMQP messages.
      def handler(channel, method, properties, body):
         
         # Process the given AMQP message.
         payload = json.loads(body)
         process_log_line(ses, bucket, target_directory,
                          temp_directory, payload)
      
      channel.basic_consume(handler, queue_name, no_ack = True)
   
   finally:
      # Clean up after ourselves.
      shutil.rmtree(temp_directory)

if __name__ == "__main__":
   main()