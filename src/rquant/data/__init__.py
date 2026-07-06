"""随包分发的数据文件（进 git，随代码到云端）。

当前仅 ``intraday_progress_curve.json``（盘中累计成交额进度曲线，scripts/
calibrate-intraday-curve.py 从本地 minute_bar 标定产出）。用 importlib.resources
定位包内文件，兼容源码 PYTHONPATH 运行与 editable 安装两种形态。
"""
