from template.page import *
from time import time
from template.index import Index
from template.config import *


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        list = '['
        for column in self.columns:
            list = list + str(column) + ', '

        list = list[0:len(list)-2]

        list = list + ']'

        return list

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_columns = num_columns + 4

        # self.indeces = []
        # for i in range(self.num_columns):
        #     self.indeces.append({})
        self.page_directory = {}


        self.tail_page_directory = {}
        self.page_range = num_columns + 4
        self.keyToRID = {}


        self.use_count = [0] * 11
        self.clean =[0] * 11
        self.pin =[0] * 11

        self.pages = [[]]
        self.tail_pages = [[]]
        self.ranges_in_buffer = [0]
        self.range_id = 1
        for i in range(0, self.page_range):
            new_page = Page()
            self.pages[0].append(new_page)
        for i in range(0, self.total_columns):
            new_page = Page()
            self.tail_pages[0].append(new_page)

        self.index = Index(self)

        pass


 
