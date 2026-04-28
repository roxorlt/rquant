"""rQuant 通知模块：PushDeer 推送 + 5 类场景路由。

入口：notify(scene, **kwargs)
失败：写日志，不阻塞业务
"""

from rquant.notify.api import notify

__all__ = ["notify"]
