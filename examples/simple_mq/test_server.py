from time import sleep, time
from vnpy.mq import MqServer


class TestMq(MqServer):
    def __init__(self):
        """
        Constructor
        """
        super(TestMq, self).__init__()
    
    def callback(self, topic: str, data: Any) -> None:
        print('topic is %s' % topic.decode('utf-8'))
        print('data is %s' % data.decode('utf-8'))


if __name__ == '__main__':
    subscribe_address = "tcp://127.0.0.1:4102"
    publish_address = "tcp://*:2222"
    mq = TestMq()
    topics = ["hi","halo"]
    mq.subscribe_topic(topics)
    mq.start(
        subscribe_address,
        publish_address
    )
    while 1:
        content = "current server time is {time()}"
        print(content)
        mq.publish("test", content)
        time.sleep(5)
