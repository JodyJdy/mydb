import struct
import sys
sys.path.append("../store")
sys.path.append("../util")
sys.path.append("../config.py")
import config
from store.values import *
from store.container import *

config.add_container("hello2")
container = Container("hello2")
p = container.new_common_page()
print(p.page_num)
container.flush()

