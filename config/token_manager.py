"""Token管理模块"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv, set_key
import tushare as ts
from config.settings import LOCAL_ENV_FILE, USER_ENV_FILE

def get_env_file():
    """获取环境变量文件路径，优先使用项目本地的 .env"""
    if LOCAL_ENV_FILE.exists():
        return LOCAL_ENV_FILE
    return USER_ENV_FILE

def init_env_file():
    """初始化环境变量文件，优先加载项目本地的 .env，然后加载用户目录的 .env"""
    # 优先加载项目本地的 .env
    if LOCAL_ENV_FILE.exists():
        load_dotenv(LOCAL_ENV_FILE, override=True)
    
    # 如果用户目录的 .env 存在，也加载（但不会覆盖已存在的变量）
    if USER_ENV_FILE.exists():
        load_dotenv(USER_ENV_FILE, override=False)
    else:
        # 如果用户目录的 .env 不存在，创建目录结构
        USER_ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not USER_ENV_FILE.exists():
            USER_ENV_FILE.touch()

def get_tushare_token() -> Optional[str]:
    """获取Tushare token，优先从项目本地 .env 读取，然后从用户目录读取"""
    init_env_file()
    # 先从环境变量中获取（已通过 load_dotenv 加载）
    token = os.getenv("TUSHARE_TOKEN")
    
    # 如果环境变量中没有，尝试直接从文件读取
    if not token:
        env_file = get_env_file()
        if env_file.exists():
            load_dotenv(env_file, override=True)
            token = os.getenv("TUSHARE_TOKEN")
    
    return token

def set_tushare_token(token: str, use_local: bool = True):
    """
    设置Tushare token
    
    参数:
        token: Tushare API token
        use_local: 是否保存到项目本地 .env 文件（默认 True），否则保存到用户目录
    """
    init_env_file()
    
    # 根据 use_local 参数选择保存位置
    if use_local:
        env_file = LOCAL_ENV_FILE
    else:
        env_file = USER_ENV_FILE
        env_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 如果文件不存在，创建它
    if not env_file.exists():
        env_file.touch()
    
    set_key(env_file, "TUSHARE_TOKEN", token)
    # 初始化tushare
    ts.set_token(token)

