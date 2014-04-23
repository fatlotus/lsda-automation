import pika
from boto.ec2.autoscale import AutoScaleConnection
import time
import logging
import yaml

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
        level = logging.DEBUG
    )

    # Read configuration.
    options = yaml.load(open("config.yaml"))

    # Connect to the RabbitMQ cluster.
    params = pika.ConnectionParameters(host=options["amqp"])
    conn = pika.BlockingConnection(params)

    channel = conn.channel()

    while True:
        # See if we have 30s of busy waiting.
        for i in xrange(6):
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

            time.sleep(120)

        # Wait until next polling event.
        time.sleep(30)

if __name__ == '__main__':
    main()