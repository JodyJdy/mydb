import struct
import sys
import time

from store.values import *
from store.container import *
from store.log.binlog import binlog

config.add_container("helloworld")
#page 1 总是存放 btree的信息
container = Container.open_container("helloworld")

def example1():
    p = container.new_common_page()
    p.insert_slot(Row([StrValue.none()]),0)
    p.update_by_record_id(generate_row(["hello"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworld"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworldslkdjf"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(Row([StrValue.none()]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworld"]),0)
    print(p.read_slot(0))

def example2():
    p = container.new_common_page()
    p.insert_slot(Row([StrValue.none()]),0)
    p.update_by_record_id(generate_row(["hello"*100]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworld"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworldslkdjf"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(Row([StrValue.none()]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworld"]),0)
    print(p.read_slot(0))
    p.update_by_record_id(generate_row(["helloworld"*20]),0)
    print(p.read_slot(0))


def example3():
    """
    测试刷新脏页时长， 页面越多 耗时越多
    :return:
    """
    for i in range(20000):
        p = container.new_common_page()
        p.insert_slot(Row([StrValue.none()]),0)
    start = time.time()
    container.flush()
    container.close()
    print(time.time() -start)

