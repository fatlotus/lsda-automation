import pika
from boto.ec2.autoscale import AutoScaleConnection
import time
import logging
import yaml

DELAY = 5 * 60

def get_queue_length(channel, queue_name):
    """
    Measures the length of the given queue.
    """

    return channel.queue_declare(
        queue = queue_name,
        durable = True,
        exclusive = False,
        auto_delete = False,
        passive = True
    ).method.message_count

def main():
    """
    Main entry point for the automated scaling daemon.
    """

    # Configure logging.
    logging.basicConfig(
        format = "%(asctime)-15s %(levelname)5s %(message)s",
        level = logging.INFO
    )

    # Read configuration.
    options = yaml.load(open("config.yaml"))

    # Connect to the RabbitMQ cluster.
    params = pika.ConnectionParameters(host=options["amqp"])
    conn = pika.BlockingConnection(params)

    channel = conn.channel()

    while True:
        # Ensure that we have things stuck in the queue for the given amount
        # of time.
        for i in xrange(DELAY / 5):
            queue_length = get_queue_length(channel, "stable")

            logging.info("Queue length: {}".format(queue_length))

            if queue_length == 0:
                break
            time.sleep(5)

        else:
            # Scale up!
            group = AutoScaleConnection().get_all_groups(["LSDA Worker Pool"])[0]
            group.desired_capacity = min(
              group.desired_capacity + 2, group.max_size)
            group.update()

            logging.info(
              "Triggering increase to {}".format(group.desired_capacity))

            time.sleep(300)

        # Wait until next polling event.
        time.sleep(30)

if __name__ == '__main__':
    main()