import os
from os import truncate
import signal
import threading
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict
from pathlib import Path

import zmq
import zmq.auth
from zmq.backend.cython.constants import NOBLOCK
from zmq.auth.thread import ThreadAuthenticator

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
        # Save functions dict: key is function name, value is function object
        self.__functions: Dict[str, Any] = {}

        # Zmq port related
        self.__context: zmq.Context = zmq.Context()

        # Subscribe socket (subscribe pattern)
        self.__socket_subscribe: zmq.Socket = self.__context.socket(zmq.SUB)

        # Publish socket (Publish pattern)
        self.__socket_pubish: zmq.Socket = self.__context.socket(zmq.PUB)

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
            publish_address: str,
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

    # def run(self) -> None:
    #     """
    #     Run MqServer functions
    #     """
    #     start = datetime.utcnow()
    #
    #     while self.__active:
    #         # Use poll to wait event arrival, waiting time is 1 second (1000 milliseconds)
    #         cur = datetime.utcnow()
    #         delta = cur - start
    #
    #         if delta >= KEEP_ALIVE_INTERVAL:
    #             self.publish(KEEP_ALIVE_TOPIC, cur)
    #
    #         if not self.__socket_subscribe.poll(1000):
    #             continue
    #
    #         # Receive request data from Reply socket
    #         req = self.__socket_rep.recv_pyobj()
    #
    #         # Get function name and parameters
    #         name, args, kwargs = req
    #
    #         # Try to get and execute callable function object; capture exception information if it fails
    #         try:
    #             func = self.__functions[name]
    #             r = func(*args, **kwargs)
    #             rep = [True, r]
    #         except Exception as e:  # noqa
    #             rep = [False, traceback.format_exc()]
    #
    #         # send callable response by Reply socket
    #         self.__socket_rep.send_pyobj(rep)
    #
    #     # Unbind socket address
    #     self.__socket_pub.unbind(self.__socket_pub.LAST_ENDPOINT)
    #     self.__socket_rep.unbind(self.__socket_rep.LAST_ENDPOINT)
    #
    #     if self.__authenticator:
    #         self.__authenticator.stop()

    def run(self) -> None:
        pull_tolerance = int(KEEP_ALIVE_TOLERANCE.total_seconds() * 1000)
        while self.__active:
            if not self.__socket_subscribe.poll(pull_tolerance):
                self.on_disconnected()
                continue

            # Receive request data from Reply socket
            req = self.__socket_subscribe.recv_pyobj()
            print("et msg")
            print(req)

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

    def subscribe_topic(self, topic: str) -> None:
        """
        Subscribe data
        """
        self.__socket_subscribe.set_string()

    def on_disconnected(self):
        """
        Callback when heartbeat is lost.
        """
        print("RpcServer has no response over {tolerance} seconds, please check you connection."
              .format(tolerance=KEEP_ALIVE_TOLERANCE.total_seconds()))

