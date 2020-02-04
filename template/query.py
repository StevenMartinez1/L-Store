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
        self.tailRIDcount = 2**64 -1
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
        current_page = i
        indirection = 257777
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

        j = 0
        for column in columns:

                if(j == self.table.key):
                    key = column
                self.table.pages[i].write(column)

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
        recordList = []
        updateRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[updateRID]

        record = []

        j = 0;
        for i in range(4,self.table.num_columns):
            if(query_columns[j] == 0):
                record.append(None)
            else:
                record.append(self.readRecord(page + i, offset))
            j = j + 1

        recordList.append(record)

        print(recordList[0])

        return recordList
        



        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        self.updateRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]

        recordIndirection = self.readRecord(page, offset)
        if(recordIndirection == 0):
            hasUpdated = False
        else:
            hasUpdated = True

        i = 0
        while(self.table.tail_pages[i].num_records == 512):
            if(self.table.tail_pages[i].num_records == 512):
                i = i + self.table.num_columns

        if(hasUpdated):
            tailIndirection = recordIndirection
        else:
            tailIndirection = 0

        self.table.tail_pages[i].write(tailIndirection)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1

        tailRID = self.tailRIDcount
        self.tailRIDcount = self.tailRIDcount - 1
        self.table.tail_pages[i].write(tailRID)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1

        # base page indirection = newly created tail page record RID
        self.table.pages[page].writeAtOffset(tailRID, offset)


         # timeStamp = time()
        self.table.tail_pages[i].write(0)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1

        schema_encoding = 0;
        self.table.tail_pages[i].write(schema_encoding)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1

        for column in columns:
            if(column != None):
                self.table.tail_pages[i].write(column)
            if(column == None):
                column_index = i % self.table.num_columns
                basePageRecordColumn = self.readRecord(page + column_index, offset)
                self.table.tail_pages[i].write(basePageRecordColumn)
            if (self.table.tail_pages[i].num_records == 512):
                new_page = Page()
                self.table.tail_pages.append(new_page)
            i = i + 1

        pass



    def readRecord(self, page, offset):
        eightByteVal = ''
        for i in range(8):
            binaryByte = bin((self.table.pages[page].data[offset+i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8-len(byte)) * '0' + byte
            eightByteVal = eightByteVal + byte;

        recordVal = int(eightByteVal, 2)
        return recordVal


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass
