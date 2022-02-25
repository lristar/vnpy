from __future__ import print_function
from __future__ import absolute_import
from time import sleep, time
from vnpy.mq import MqServer


class TestMq(MqServer):
    def __init__(self):
        """
        Constructor
        """
        super(TestMq, self).__init__()


if __name__ == '__main__':
    subscribe_address = "tcp://*:2014"
    publish_address = "tcp://*:4102"
    mq = TestMq()
    mq.start(
        subscribe_address,
        publish_address,
    )
    while 1:
        content = f"current server time is {time()}"
        print(content)
        mq.publish("test", content)
        sleep(5)
