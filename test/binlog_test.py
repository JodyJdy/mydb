import sys
from store.disk_tree import *
from store.log.binlog import binlog

from store.container import *
from store.log.binlog import binlog, PhysicalPageLogEntry
def del_tree(t: BTree, start, end):
    for i in range(start, end):
        t.delete(generate_row([i + 1]))

def test_count(t:BTree):
    node2 = t.search(generate_row([0]))
    count = 0
    while node2:
        count += node2.row_num()
        if node2.row_num() == 0:
            pass
        else:
            for i in range(node2.row_num()):
                print(node2.get_row_i(i), end=',')
            print()
        node2 = node2.get_right_node()
    print(count)

def write_data_without_container_flush(name:str):
    info = BTreeInfo(name,-1,1,False,[IntValue])
    BTree.create_btree(info,True)
    t = BTree.open_btree(name)
    for i in range(500):
        t.insert(generate_row([i]))
    for i in range(200,400):
        t.delete(generate_row([i]))
    test_count(t)
    binlog.flush()

def test_tree(name:str):
    info = BTreeInfo(name,-1,1,False,[IntValue])
    BTree.create_btree(info,True)
    t = BTree.open_btree(name)
    test_count(t)

def recovery(name:str):
    container = Container.open_container(name,False)
    for i in binlog.read_log_entry(0):
        i:PhysicalPageLogEntry
        page = container.get_page(i.page_id)
        if i.get_entry_pos() >= page.lsn:
            print(f" recover page lsn :{page.lsn}, entry pos{i.get_entry_pos()} ")
            page_data_len = len(i.data)
            page.page_data[i.offset:i.offset+page_data_len] = i.data
            page.dirty = True
    container.flush()
    container.close()


# 1. 写入数据，不flush container
# write_data_without_container_flush("tree1")
# 2. binlog 恢复数据
#recovery("tree1")
# 3.检查数据是否恢复
# test_tree("tree1")