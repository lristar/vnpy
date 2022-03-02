
from ast import Pass
from dataclasses import dataclass
import os
from os import truncate
import signal
import threading
from tkinter.messagebox import NO
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
import turtle
from typing import Any, Callable, Dict
from pathlib import Path
import sys
from numpy import meshgrid
from sqlalchemy import false, true
import zmq
import datetime
import json
from zmq.backend.cython.constants import NOBLOCK
import time

# Achieve Ctrl-c interrupt recv
signal.signal(signal.SIGINT, signal.SIG_DFL)

KEEP_ALIVE_TOPIC: str = "_keep_alive"
KEEP_ALIVE_INTERVAL: timedelta = timedelta(seconds=1)
KEEP_ALIVE_TOLERANCE: timedelta = timedelta(seconds=30)
pull_tolerance = int(KEEP_ALIVE_TOLERANCE.total_seconds() * 3000)


class RemoteException(Exception):
    """
    RPC remote exception
    """

    def __init__(self, value: Any):
        """
        Constructor
        """
        self.__value = value

    def __str__(self):
        """
        Output error message
        """
        return self.__value


class MqServer:
    """"""

    def __init__(self):
        """
        Constructor
        """
        # Zmq port related
        self.__context: zmq.Context = zmq.Context()

        # Subscribe socket (subscribe pattern)
        self.__socket_subscribe: zmq.Socket = self.__context.socket(zmq.SUB)

        # Req socket (Req pattern)
        self.__socket_req: zmq.Socket = self.__context.socket(zmq.REQ)
        
        self.__reconnect_num:0
        
        # monitor socket
        self.__socket_monitor:None

        # Worker thread related
        self.__active: bool = False  # MqServer status
        self.__sub__thread: threading.Thread = None  # SubServer thread
        self.__check__thread: threading.Thread = None # CheckServer thread
        self.__lock: threading.Lock = threading.Lock()

    def is_active(self) -> bool:
        """"""
        return self.__active

    def start(
            self,
            subscribe_address: str,
            req_address: str
            # server_secretkey_path: str = "",
            # username: str = "",
            # password: str = ""
    ) -> None:
        """
        Start RpcServer
        """
        if self.__active:
            return

        # Bind and Listen
        self.__socket_subscribe.connect(subscribe_address)
        self.__socket_req.connect(req_address)
        
        # set monitor
        self.set_monitor()

        # Start MqServer status
        self.__active = True

        # Start RpcServer thread
        self.__sub__thread = threading.Thread(target=self.run)
        self.__sub__thread.start()
    def pop(self):
        self.__socket_monitor
    
    def stop(self) -> None:
        """
        Stop RpcServer
        """
        if not self.__active:
            return

        # Stop RpcServer status
        self.__active = False

    def join(self) -> None:
        # Wait for RpcServer thread to exit
        if self.__sub__thread and self.__sub__thread.is_alive():
            self.__sub__thread.join()
        self.__sub__thread = None
        
        if self.__check__thread and self.__check__thread.is_alive():
            self.__check__thread.join()
        self.__check__thread = None

    def run(self) -> None:
        while self.__active:
            # Receive request data from Reply socket
            topic, msg = self.__socket_subscribe.recv_multipart()
            data = json.loads(msg.decode('utf-8'))
            is_success = self.request(data['time_sec'].encode('utf-8'))
            if is_success:
                self.callback(topic, msg)
            self.on_disconnected
            
            # Process data by callable function

        # Close And Unbind
        self.__socket_subscribe.close()
        self.__socket_req.close()
    
    def request(self, data: Any) -> bool:
        """
        Publish data
        """
        with self.__lock:
            self.__socket_req.send(data)
            # Timeout reached without any data
            n = self.__socket_req.poll(pull_tolerance)
            if not n:
                msg = f"Timeout of {pull_tolerance}ms reached for {data}"
                print(msg)
                return false
            rep = self.__socket_req.recv()
            print("rep is ",rep)
            if data != rep:
                print("out of sync")
                return false
            return true
        
            
    def callback(self, topic: str, data: Any) -> None:
        """
        Callable function
        """
        raise NotImplementedError

    def subscribe_topic(self, topics=[]) -> None:
        """
        Subscribe data
        """
        if len(topics) == 0:
            print("Receiving messages on ALL topics...")
            self.__socket_subscribe.setsockopt(zmq.SUBSCRIBE, b'')
        else:
            print("Receiving messages on topics: %s ..." % topics)
            for t in topics:
                self.__socket_subscribe.setsockopt(zmq.SUBSCRIBE, t.encode('utf-8'))
            self.__socket_subscribe.setsockopt(zmq.SUBSCRIBE, "ping".encode('utf-8'))    
    def set_reconnect(self) -> None:
        pass
    
    def set_monitor(self):
        self.__socket_monitor = self.__socket_subscribe.get_monitor_socket()

    def on_disconnected(self):
        """
        Callback when heartbeat is lost.
        """
        print("RpcServer has no response over {tolerance} seconds, please check you connection."
              .format(tolerance=KEEP_ALIVE_TOLERANCE.total_seconds()))
        

class TestMq(MqServer):
    def __init__(self):
        """
        Constructor
        """
        super(TestMq, self).__init__()
    
    def callback(self, topic: str, data: Any) -> None:
        tp = topic.decode('utf-8')
        # if tp !="ping":
        print('topic is %s' % topic.decode('utf-8'))
        print('data is %s' % data.decode('utf-8'))


if __name__ == '__main__':
    subscribe_address = "tcp://127.0.0.1:4102"
    req_address = "tcp://127.0.0.1:2222"
    mq = TestMq()
    topics = ["hi","halo"]
    mq.subscribe_topic(topics)
    mq.start(
        subscribe_address,
        req_address
    )
    


