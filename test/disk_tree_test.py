import sys
sys.path.append("../store")
sys.path.append("../util")
sys.path.append("../config.py")
from store.disk_tree import *

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

def test_tree():
    info = BTreeInfo("my_tree2",-1,1,False,[IntValue])
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree2")
    for i in range(0,60):
        t.insert(generate_row([i]))
    t.show()
    test_count(t)
    t.container.flush()
    t.container.close()


test_tree()
