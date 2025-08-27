import struct
import sys
from store.values import *
from store.container import *
from store.log.binlog import binlog
config.add_container("hello2")
container = Container("hello2",True)
p = container.new_common_page()
p.insert_slot(generate_row(["hello"]),0)
print(p.page_num)
container.flush()

