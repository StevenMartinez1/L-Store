from template.page import *
from time import time
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
        self.page_directory = {}
        self.tail_page_directory = {}

        self.pages = []
        self.tail_pages = []
        for i in range(0, self.num_columns+4):
            new_page = Page()
            self.pages.append(new_page)
        for i in range(0, self.num_columns+4):
            new_page = Page()
            self.tail_pages.append(new_page)

        pass

    def __merge(self):
        pass
 
