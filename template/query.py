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
        #self.RIDcount_tail = pow(2,64) - 1
        self.tailRIDcount = 2 ** 64 - 1
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):

        self.updateRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]


        recordIndirection = self.readRecord(page, offset)
        while(recordIndirection != 0):
            for l in range(0, self.table.num_columns):
                print(self.readRecord(page + l, offset), end=" ")
            (page2, offset2) = self.table.page_directory[recordIndirection]
            recordIndirection = self.readTailRecord(page2, offset2)
            for j in range(0, self.table.num_columns):
                self.table.tail_pages[page2 + j].writeAtOffset(0, offset2)


        for i in range(0, self.table.num_columns):
            self.table.pages[page + i].writeAtOffset(0, offset)
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

        """
        self.currentRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[self.currentRID]
        # print out all the values in the table base pages
        for l in range(0, self.table.num_columns):
            print(self.readRecord(page + l ,offset), end =" ")
        """
        pass


    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        recordList = []
        RID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[RID]

        recordValues = []

        recordIndirection = self.readRecord(page, offset)

        if(recordIndirection == 0):
            hasUpdated = False
        else:
            hasUpdated = True

        if(hasUpdated == False):
            j = 0
            for i in range(4,self.table.num_columns):
                if(query_columns[j] == 0):
                    recordValues.append(None)
                else:
                    recordValues.append(self.readRecord(page + i, offset))
                j = j + 1

            record = Record(RID,key, recordValues)
            recordList.append(record)
        else:
            j = 0
            for i in range(4,self.table.num_columns):
                if(query_columns[j] == 0):
                    recordValues.append(None)
                else:
                    #print(recordIndirection)
                    (tailPage, tailOffset) = self.table.tail_page_directory[recordIndirection]

                    #self.table.tail_page_directory[tailRID]

                    #print('tailRID', recordIndirection)
                    #print('tailPage and offset', tailPage, offset)
                    recordValues.append(self.readTailRecord(tailPage + i, tailOffset))
                    #print(self.readRecord(tailPage + i, tailOffset))
                j = j + 1

            record = Record(RID,key, recordValues)
            recordList.append(record)

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
        current_page = i

        if(hasUpdated):
            tailIndirection = recordIndirection
        else:
            tailIndirection = 0


        self.table.tail_pages[i].write(tailIndirection)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        #
        # Need to change base page indirection and schema encoding




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
        #print(self.readRecord(page, offset))
        #print(tailRID)

        # timeStamp = time()
        self.table.tail_pages[i].write(0)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1


        schema_encoding = 0

        self.table.tail_pages[i].write(schema_encoding)
        if (self.table.tail_pages[i].num_records == 512):
            new_page = Page()
            self.table.tail_pages.append(new_page)
        i = i + 1

        for column in columns:
                if(column != None):
                        self.table.tail_pages[i].write(column)
                if(column == None):
                    if(tailIndirection == 0):
                        column_index = i % self.table.num_columns
                        basePageRecordColumn = self.readRecord(page + column_index, offset)
                        self.table.tail_pages[i].write(basePageRecordColumn)
                    if(tailIndirection != 0):
                        column_index = i % self.table.num_columns
                        (page_tail, offset_tail) = self.table.tail_page_directory[tailIndirection]
                        insert_value = self.readTailRecord(page_tail + column_index, offset_tail)
                        self.table.tail_pages[i].write(insert_value)



                if (self.table.tail_pages[i].num_records == 512):
                        new_page = Page()
                        self.table.tail_pages.append(new_page)
                i = i + 1

        #self.index.keyToRID[key] = RID
        self.table.tail_page_directory[tailRID] = (current_page,     (self.table.tail_pages[current_page].num_records - 1) * 8       )


        # print out all the values in the table tail pages
        #self.currentRID = self.index.keyToRID[key]
        #(page, offset) = self.table.page_directory[self.currentRID]
        #for l in range(0, self.table.num_columns):
        #   print(self.readTailRecord(i - self.table.num_columns + l , (self.table.tail_pages[i-1].num_records * 8 - 8)), end =" ")
        pass

# it only reads from base pages
    def readRecord(self, page, offset):
        eightByteVal = ''
        for i in range(8):
            binaryByte = bin((self.table.pages[page].data[offset + i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8 - len(byte)) * '0' + byte
            eightByteVal = eightByteVal + byte;
        recordVal = int(eightByteVal, 2)

        return recordVal

    def readTailRecord(self, page, offset):
        eightByteVal = ''
        for i in range(8):
            binaryByte = bin((self.table.tail_pages[page].data[offset + i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8 - len(byte)) * '0' + byte
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
