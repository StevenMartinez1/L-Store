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
        self.index = Index(table)
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
        
        #Look for a page to put your record
        i = 0
        while(self.table.pages[i].num_records == 512):
            if(self.table.pages[i].num_records == 512):
                i = i + self.table.num_columns
        current_page = i #why does this need to be 0? because thats what it stays as
        #when current page is used later it is 0, or whatever i is rn.
        
        #indirection
        indirection = 0
        self.table.pages[i].write(indirection) #place indirection
        #check if we filled up a page with our insertion
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1 #move to the next column of data

        #RID
        RID = self.RIDcount
        self.RIDcount = self.RIDcount + 1 #increment number of records we have
        self.table.pages[i].write(RID)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1 #move to the next column of data

        #timeStamp = time()
        self.table.pages[i].write(0)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1 #move to the next column of data

        #Schema Encoding
        self.table.pages[i].write(schema_encoding)
        if (self.table.pages[i].num_records == 512):
            new_page = Page()
            self.table.pages.append(new_page)
        i = i + 1 #move to the next column of data

        #now we enter the data that the user provided.
        j = 0
        for column in columns: #for as long as there are items in the list
            if(j == 0): #first item: meanning it should be the key
                key = column
            #add the item to a page
            self.table.pages[i].write(column)
            #if we filled up the page, then make another one!
            if (self.table.pages[i].num_records == 512):
                    new_page = Page()
                    self.table.pages.append(new_page)
            i = i + 1
            j = j + 1
        self.index.keyToRID[key] = RID
        self.table.page_directory[RID] =(current_page,     (self.table.pages[current_page].num_records - 1) * 8       )
        #print(self.table.page_directory)
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
        self.updateRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]




        i = 0
        while(self.table.tail_pages[i].num_records == 512):
            if(self.table.tail_pages[i].num_records == 512):
                i = i + self.table.num_columns


        #print(key)
        #print(self.updateRID)
        #print(page, offset)
        #print(columns)


        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
