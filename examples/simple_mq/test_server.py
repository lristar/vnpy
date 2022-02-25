from __future__ import print_function
from __future__ import absolute_import
from time import sleep

from vnpy.mq import MqServer


class TestMq(MqServer):
    def __init__(self):
        """
        Constructor
        """
        super(TestMq, self).__init__()



if __name__ == '__main__':
    TestMq()

