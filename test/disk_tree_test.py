import sys
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

# 普通增删改查
def test_tree1():
    info = BTreeInfo("my_tree1",-1,1,False,[IntValue,StrValue])
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree1")
    # print(t.search(generate_row([1])).get_row_i(0))
    for i in range(2000):
        t.insert(generate_row([i, "hello"]))
    for i in range(200,500):
        t.delete(generate_row([i]))

    for i in range(700,800):
        t.delete(generate_row([i]))

    for i in range(1000,1200):
        t.delete(generate_row([i]))
    test_count(t)
    t.container.flush()
    t.container.close()

# 增删改查2
def test_tree2():
    info = BTreeInfo("my_tree2",-1,1,False,[IntValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
                                            ]
                     )
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree2")
    # print(t.search(generate_row([1])).get_row_i(0))
    for i in range(1000):
        t.insert(generate_row([i,
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none()
                               ]))

    for i in range(1000):
        t.insert(generate_row([i,
                               "hello","hello","hello","hello","hello","hello"
                               ]))

    for i in range(1000):
        t.insert(generate_row([i,
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none()
                               ]))

    for i in range(300,800):
        t.delete(generate_row([i]))
    test_count(t)
    t.container.flush()
    t.container.close()

# 增删改查3 长字段修改
def test_tree3():
    info = BTreeInfo("my_tree3",-1,1,False,[IntValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
                                            ]
                     )
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree3")
    # print(t.search(generate_row([1])).get_row_i(0))
    for i in range(1000):
        t.insert(generate_row([i,
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               ]))

    for i in range(1000):
        t.insert(generate_row([i,
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               "hello","hello","hello","hello","hello","hello",
                               ]))

    for i in range(1000):
        t.insert(generate_row([i,
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),StrValue.none(),
                               ]))

    for i in range(300,800):
        t.delete(generate_row([i]))
    test_count(t)
    t.container.flush()
    t.container.close()


#
def test_tree4():
    info = BTreeInfo("my_tree4",-1,1,False,[IntValue
        ,StrValue,StrValue,StrValue,StrValue,StrValue,StrValue
                                            ]
                     )
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree4")
    # print(t.search(generate_row([1])).get_row_i(0))

    for i in range(1000):
        t.insert(generate_row([i,
                               "hello","hello","hello","hello","hello","hello",
                               ]))

    for i in range(1000):
        t.insert(generate_row([i,
                               "hello"*100,
                               "hello"*100,
                               "hello"*100,
                               "hello"*200,
                               "hello"*200,
                               "hello"*200,
                               ]))

    for i in range(300,800):
        t.delete(generate_row([i]))
    test_count(t)
    t.container.flush()
    t.container.close()


def test_tree5():
    """
    测试 排序
    :return:
    """
    info = BTreeInfo("my_tree5",-1,1,True,[IntValue,IntValue])
    BTree.create_btree(info,True)
    t = BTree.open_btree("my_tree5")
    # t.insert(generate_row([1,1]))
    # t.insert(generate_row([1,2]))
    # t.insert(generate_row([1,3]))
    # t.insert(generate_row([1,5]))
    # t.insert(generate_row([1,6]))
    # t.insert(generate_row([1,7]))
    # t.insert(generate_row([1,8]))
    # t.insert(generate_row([1,9]))
    # t.insert(generate_row([1,10]))
    test_count(t)
    # node = t.search_part(generate_row([1]),asc=True)
    # for i in range(node.row_num()):
    #     print(node.get_row_i(i))
    t.container.flush()
    t.container.close()




