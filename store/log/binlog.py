import config
from logger import RotatingLogger

class BinLog:
    def __init__(self):
        self.logger = RotatingLogger("binlog",config.LOG_FILE_PER_SIZE,config.LOG_BUFFER_SIZE)
