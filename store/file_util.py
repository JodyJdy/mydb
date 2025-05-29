import os


def create_file_if_need(path:str):
    if not os.path.exists(path):
        file = open(path,'w')
        file.close()

def create_dir_if_need(path:str):
    if not os.path.exists(path):
        os.makedirs(path)
