import json
import os
from pathlib import Path

# 页的大小 min page size=173 所有btree中的页全部不存放数据的极端情况,页面大小配置不能超过 32KB
PAGE_SIZE = 200
# btree信息存放的页
BTREE_INFO_PAGE_NUM = 1
# 存放所有container的目录
CONTAINER_PATH =  Path(__file__).parent / "data"
# meta path
META_PATH = Path(__file__).parent / "data"/"meta.json"


OPEN_BINLOG = False



def create_container_if_need(container_name: str):
    container_path = Path(CONTAINER_PATH)
    if not container_path.exists():
        container_path.mkdir(parents=True)
    container_file = container_path / container_name
    if not container_file.exists():
        file = open(container_file, 'w')
        file.close()
    return open(container_file, 'rb+')

def read_container(container_name: str):
    path = Path(CONTAINER_PATH) / container_name
    return open(path, 'rb+')


def container_size(container_name: str) -> int:
    path = Path(CONTAINER_PATH) / container_name
    return os.path.getsize(path)

def write_meta_data(data):
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def read_meta_data():
    if not META_PATH.exists():
        meta_data ={
            # container 名称 和 id 的映射关系
            "container_name_to_id": {
            },
            "container_id_to_name": {
            },
            # redo log 配置
            "redo_log":{
                "lsn":0,
                "write_pos":0,
                "check_point":0
            }
        }
        write_meta_data(meta_data)
        return meta_data

    with open(META_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

_meta_data = read_meta_data()


def add_container(container:str):
    if container not in _meta_data["container_name_to_id"]:
        container_id = len(_meta_data["container_name_to_id"])
        _meta_data["container_name_to_id"][container] = container_id
        _meta_data["container_id_to_name"][container_id] = container
        write_meta_data(_meta_data)

def get_container_id(container_name:str):
    return  _meta_data["container_name_to_id"][container_name]

def get_container_name(container_id:int):
    return _meta_data["container_id_to_name"][str(container_id)]

def container_exists(container_name: str) -> bool:
    return  container_name in _meta_data["container_name_to_id"]


# 单日志文件大小 100MB
LOG_FILE_PER_SIZE = 100 * 1024 * 1024
# 日志缓冲大小 10 MB
LOG_BUFFER_SIZE = 10 * 1024 * 1024
#日志文件路径
LOG_FILE_PATH =   Path(__file__).parent / "data" / "logfile"

def create_log_directory_if_need():
    log_path = Path(LOG_FILE_PATH)
    if not log_path.exists():
        log_path.mkdir(parents=True)
