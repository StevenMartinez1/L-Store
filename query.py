from template.table import Table, Record
from template.index import Index
from template.page import *
from template.config import *
from time import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """


    def __init__(self, table):
        self.table = table
        self.RIDcount = 1
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        #schema_encoding = '0' * self.table.num_columns
        schema_encoding = 0

        #newRecord = Record(RIDcount, key,  )
        i = 0
        while(self.table.pages[i].num_records == 512):
            if(self.table.pages[i].num_records == 512):
                i = i + self.table.num_columns

        indirection = 0
        self.table.pages[i].write(indirection)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1


        RID = self.RIDcount
        self.RIDcount = self.RIDcount + 1
        self.table.pages[i].write(RID)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1

        #timeStamp = time()
        self.table.pages[i].write(0)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1

        self.table.pages[i].write(schema_encoding)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1


        for column in columns:

                self.table.pages[i].write(column)

                if (self.table.pages[i].num_records == 512):
                        new_page = Page()
                        self.table.pages.append(new_page)
                i = i + 1
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):

        
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
