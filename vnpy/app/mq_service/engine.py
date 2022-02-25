import traceback
from typing import Optional

from vnpy.event import Event, EventEngine
from vnpy.mq import MqServer
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.object import LogData

APP_NAME = "MqService"

EVENT_MQ_LOG = "eMqLog"


class MqEngine(BaseEngine):
    setting_filename = "mq_service_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.publish_address = "tcp://*:2014"
        self.subscribe_address = "tcp://*:4102"

        self.server: Optional[MqServer] = None
        self.init_server()
        self.load_setting()
        self.register_event()

    def init_server(self):
        """
        init MqServer
        :return: None
        """
        self.server = MqServer()
        self.server.register(self.main_engine.subscribe)
        self.server.register(self.main_engine.send_order)
        self.server.register(self.main_engine.cancel_order)
        self.server.register(self.main_engine.query_history)

    def load_setting(self):
        """"""
        setting = load_json(self.setting_filename)
        self.subscribe_address = setting.get("subscribe_address", self.subscribe_address)
        self.publish_address = setting.get("publish_address", self.publish_address)

    def save_setting(self):
        """"""
        setting = {
            "subscribe_address": self.subscribe_address,
            "publish_address": self.publish_address
        }
        save_json(self.setting_filename, setting)

    def start(self, subscribe_address: str, publish_address: str):
        """"""
        if self.server.is_active():
            self.write_log("MQ服务运行中")
            return False

        self.subscribe_address = subscribe_address
        self.publish_address = publish_address

        try:
            self.server.start(subscribe_address, publish_address)
        except:  # noqa
            msg = traceback.format_exc()
            self.write_log(f"MQ服务启动失败：{msg}")
            return False

        self.save_setting()
        self.write_log("MQ服务启动成功")
        return True

    def stop(self):
        """"""
        if not self.server.is_active():
            self.write_log("MQ服务未启动")
            return False

        self.server.stop()
        self.server.join()
        self.write_log("MQ服务已停止")
        return True

    def close(self):
        """"""
        self.stop()

    def register_event(self):
        """"""
        self.event_engine.register_general(self.process_event)

    def process_event(self, event: Event):
        """"""
        if self.server.is_active():
            self.server.publish("", event)

    def write_log(self, msg: str) -> None:
        """"""
        log = LogData(msg=msg, gateway_name=APP_NAME)
        event = Event(EVENT_MQ_LOG, log)
        self.event_engine.put(event)
