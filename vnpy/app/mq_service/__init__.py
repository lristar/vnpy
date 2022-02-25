from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine import MqEngine, APP_NAME


class MqServiceApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    #  current file path
    app_path = Path(__file__).parent
    display_name = "MQ服务"
    # T
    engine_class = MqEngine
    widget_name = "MQManager"
