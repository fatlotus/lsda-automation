#!/usr/bin/env python
#
# A persistent worker daemon to notify me with regular updates of how much quota
# is left for each user.
# 
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 12 January 2013

import boto.ses
from kazoo.client import KazooClient

import datetime, time, yaml, logging

def trigger_update(zookeeper, email_connection):
   """
   Sends an email to me detailing the current status of the quotas in the fleet.
   """
   
   # List all resources in ZooKeeper.
   resources = zookeeper.get_children("/quota_limit")
   results = []
   
   for resource in resources:
      # Retreive the list of all CNetIDs.
      cnetids = zookeeper.get_children("/quota_limit/{resource}".
                                         format(**locals()).encode("utf-8"))
      
      results.append("## {:<17} {:>15} {:>15}".
                          format(resource, "USED", "LIMIT"))
      
      for cnetid in cnetids:
         # Find the upper limit for this user.
         try:
            limit = zookeeper.Counter("/quota_limit/{resource}/{cnetid}".
                                        format(**locals()).encode("utf-8"),
                                        default = 0.0).value
         except NoSuchNode, ValueError:
            limit = 0.0
         
         # Find the current utilization.
         try:
            used = zookeeper.Counter("/quota_used/{resource}/{cnetid}".
                                        format(**locals()).encode("utf-8"),
                                        default = 0.0).value
         except NoSuchNode, ValueError:
            used = 0.0
         
         # Display this as a ratio of usage.
         ratio = float(used) / float(limit)
         
         # Generate a new output row for this user.
         results.append(
           "{cnetid:<20} {used:15,.0f} {limit:15,.0f}  ({ratio:.0%})".format(
             **locals()))
      
      # Add two blank lines after each table.
      results += ["", ""]
   
   # Construct the message body.
   message_body = "\n".join(results)
   html_body = ("<pre style=\"font:13px Monaco,monospace;\">{0}</pre>"
     .format(message_body))
   
   # Output the message to the log.
   print(message_body)
   
   # Actually send the message.
   email_connection.send_email(
      source = "Quincy the Quota Counter <jarcher@uchicago.edu>",
      subject = "AUTO: LSDA Quota Updates",
      body = message_body,
      to_addresses = ["jarcher@uchicago.edu"],
      html_body = html_body
   )

def main():
   # Read configuration.
   options = yaml.load(open("config.yaml"))
   
   # Connect to the ZooKeeper cluster.
   zookeeper = KazooClient(
     hosts = ','.join(options["zookeeper"])
   )
   
   zookeeper.start()
   
   # Connect to SES.
   email_connection = boto.ses.connect_to_region("us-east-1")
   
   try:
      
      # Sends an update to me every six hours.
      while True:
         trigger_update(zookeeper, email_connection)
         time.sleep(48 * 3600)
      
   finally:
         
      # Clean up the connection to ZooKeeper.
      zookeeper.stop()

if __name__ == '__main__':
   main()