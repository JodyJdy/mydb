import config
from store.container import Container
from store.page import OverFlowPage
from store.values import Row, over_flow_row, generate_row


#
# f = BufferedRandom(f)
# print(f.seek(0,0))
# print(f.seekable())
# f.write(struct.pack('<i',0))
# f.write(struct.pack('<i',0))
# f.write(struct.pack('<i',0))
# f.write(struct.pack('<i',0))
# f.close()

def init_(num:int,len:int=10)->Row:
    b1 = bytearray(len)
    for i in range(len):
        b1[i] = num
    return over_flow_row(b1)
#
# print(bytes([1,2,3,4,5]))
# b = bytearray(50)
# page = OverFlowPage(1,bytearray(200))
#
# record_id,page_num = page.insert_slot(init_(1,180),0)
# #
# print(page.read_slot(0))


# y = Container('myc')
# page = y.new_page()
# print(page.page_num)
# print(page.insert_slot(generate_row(["helloworld"]),0))
# print(page.insert_slot(generate_row(['worldhello']),0))
# print(page.delete_by_slot(1))
# print(page.read_slot(0))
# print(page.read_slot(1))
# print(page.insert_slot(generate_row([1,2,3,4,5]),0))
# print(page.insert_slot(generate_row([1,2,3,4,5]),0))
# print(page.insert_slot(generate_row([1,2,3,4,5]),0))
# # print(page2.read_slot(0))
# # page.delete_by_slot(0)
# y.flush()
# y.close()

l =[1,2,3]
l.insert(0,-1)
print(l[-1])