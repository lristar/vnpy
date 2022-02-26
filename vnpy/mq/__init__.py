from os import truncate
import signal
import threading
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict
from pathlib import Path
import zmq

# Achieve Ctrl-c interrupt recv
signal.signal(signal.SIGINT, signal.SIG_DFL)

KEEP_ALIVE_TOPIC: str = "_keep_alive"
KEEP_ALIVE_INTERVAL: timedelta = timedelta(seconds=1)
KEEP_ALIVE_TOLERANCE: timedelta = timedelta(seconds=30)


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

        # Publish socket (Publish pattern)
        self.__socket_pubish: zmq.Socket = self.__context.socket(zmq.PUB)
        
        self.__reconnect_num:0
        
        # monitor socket
        self.__socket_monitor:None

        # Worker thread related
        self.__active: bool = False  # MqServer status
        self.__thread: threading.Thread = None  # MqServer thread
        self.__lock: threading.Lock = threading.Lock()

    def is_active(self) -> bool:
        """"""
        return self.__active

    def start(
            self,
            subscribe_address: str,
            publish_address: str
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
        self.__socket_pubish.bind(publish_address)
        
        # set monitor
        self.set_monitor()

        # Start MqServer status
        self.__active = True

        # Start RpcServer thread
        self.__thread = threading.Thread(target=self.run)
        self.__thread.start()

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
        if self.__thread and self.__thread.is_alive():
            self.__thread.join()
        self.__thread = None

    def run(self) -> None:
        print("run")
        pull_tolerance = int(KEEP_ALIVE_TOLERANCE.total_seconds() * 1000)
        while self.__active:
            if not self.__socket_subscribe.poll(pull_tolerance):
                self.on_disconnected()
                continue

            # Receive request data from Reply socket
            topic, msg = self.__socket_subscribe.recv_multipart()
            print("have resp")
            # Process data by callable function
            self.callback(topic, msg)

        # Close And Unbind
        self.__socket_subscribe.close()
        self.__socket_pubish.unbind(self.__socket_pubish.LAST_ENDPOINT)

    def publish(self, topic: str, data: Any) -> None:
        """
        Publish data
        """
        with self.__lock:
            self.__socket_pubish.send_pyobj([topic, data])
            

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
        


