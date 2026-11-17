import json
import os
import sys
from logger import get_logger

log = get_logger()


def _get_app_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))


_CONFIG_PATH = os.path.join(
    _get_app_dir(),
    "config.json"
)

_DEFAULT_CONFIG = {
    "my_user_id_enc": "",
    "x_pos": "center",
    "y_px": 100,
    "font_size": 14,
    "opacity": 0.7,
    "danmaku_style": "stack",
    "danmaku_speed": "mid",
    "tts_enabled": False,
    "tts_voice": "zh-CN-XiaoxiaoNeural",
    "tts_volume": 30,
    "tts_max_length": 20,
    "tts_test_text": "你好，这是一条测试语音",
}


def load_config():
    """加载配置（失败则返回默认）"""
    if not os.path.exists(_CONFIG_PATH):
        return dict(_DEFAULT_CONFIG)
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        cfg = dict(_DEFAULT_CONFIG)
        cfg.update(data)
        return cfg
    except Exception as e:
        log.warning(f"加载配置失败，使用默认值: {e}")
        return dict(_DEFAULT_CONFIG)


def save_config(cfg):
    """保存配置"""
    try:
        data = dict(_DEFAULT_CONFIG)
        data.update(cfg)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log.error(f"保存配置失败: {e}")
        return False