# 页的大小 min page size=173 所有btree中的页全部不存放数据的极端情况
import json
import os
from importlib.metadata import metadata
from pathlib import Path
PAGE_SIZE = 200
# 存放所有container的目录
CONTAINER_PATH = "container"
# 存放所有 alloc的目录
ALLOC_PATH = "alloc"
# meta path
META_PATH = Path(__file__).parent / "meta.json"



def create_alloc_if_need(container_name:str):
    alloc_name = container_name + ".alloc"
    alloc_path = Path(ALLOC_PATH)
    if not alloc_path.exists():
        alloc_path.mkdir(parents=True)
    alloc_file = alloc_path / alloc_name
    if not alloc_file.exists():
        file = open(alloc_file, 'w')
        file.close()
    return open(alloc_file, 'rb+')

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

def alloc_size(container_name: str):
    path = Path(ALLOC_PATH) / (container_name+".alloc")
    return os.path.getsize(path)

def container_size(container_name: str) -> int:
    path = Path(CONTAINER_PATH) / container_name
    return os.path.getsize(path)


def read_meta_data():
    with open(META_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

meta_data = read_meta_data()

def write_meta_data():
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta_data, f, indent=2, ensure_ascii=False)

def add_container(container:str):
    if container not in meta_data["container_name_to_id"]:
        container_id = len(meta_data["container_name_to_id"])
        meta_data["container_name_to_id"][container] = container_id
        meta_data["container_id_to_name"][container_id] = container
        write_meta_data()

def get_container_id(container_name:str):
    return  meta_data["container_name_to_id"][container_name]

def get_container_name(container_id:int):
    return meta_data["container_id_to_name"][str(container_id)]

def container_exists(container_name: str) -> bool:
    return  container_name in meta_data["container_name_to_id"]