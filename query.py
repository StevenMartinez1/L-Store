from template.table import Table, Record
from template.index import Index
from template.page import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """


    def __init__(self, table):
        self.table = table

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
        schema_encoding = '0' * self.table.num_columns

        i = 0
        while(self.table.pages[i].num_records == 512):
            if(self.table.pages[i].num_records == 512):
                i = i + self.table.num_columns



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
