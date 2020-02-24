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
        self.tailRIDcount = 2 ** 64 - 1
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):

        self.updateRID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]


        recordIndirection = self.readRecord(page,0, offset)

        # delete from tail page table
        while(recordIndirection != 0):
            (page2, offset2) = self.table.tail_page_directory[recordIndirection]
            recordIndirection = self.readTailRecord(page,page2, offset2)
            for l in range(0, self.table.total_columns):
                self.table.tail_pages[page][page2].writeAtOffset(0, offset2)


        # delete from base pages table
        for i in range(0, self.table.total_columns):
            self.table.pages[page][i].writeAtOffset(0, offset)
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        schema_encoding = 0

        i = 0
        k = 0
        while(self.table.pages[i][k].num_records == 512):
            if(self.table.pages[i][k].num_records == 512):
                i = i + 1
        current_page = i

        if(current_page == 5):
            self.write_to_disk()

        indirection = 0
        self.table.pages[i][k].write(indirection)
        if (self.table.pages[i][k].num_records == 512):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages.append([new_page])
            self.table.tail_pages.append([new_tail_page])
        k = k + 1


        RID = self.RIDcount
        self.RIDcount = self.RIDcount + 1
        self.table.pages[i][k].write(RID)
        if (self.table.pages[i][k].num_records == 512):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages[i + 1].append(new_page)
            self.table.tail_pages[i + 1].append(new_tail_page)
        k = k + 1

        timeStamp = time()
        self.table.pages[i][k].write(int(timeStamp))
        if (self.table.pages[i][k].num_records == 512):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages[i + 1].append(new_page)
            self.table.tail_pages[i + 1].append(new_tail_page)
        k = k + 1

        self.table.pages[i][k].write(schema_encoding)
        if (self.table.pages[i][k].num_records == 512):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages[i + 1].append(new_page)
            self.table.tail_pages[i + 1].append(new_tail_page)
        k = k + 1

        j = 0
        for column in columns:

                if(j == self.table.key):
                    key = column
                self.table.pages[i][k].write(column)

                if (self.table.pages[i][k].num_records == 512):
                        new_page = Page()
                        new_tail_page = Page()
                        self.table.pages[i + 1].append(new_page)
                        self.table.tail_pages[i + 1].append(new_tail_page)

                k = k + 1
                j = j + 1
        self.index.keyToRID[key] = RID
        self.table.page_directory[RID] =(current_page,     (self.table.pages[current_page][0].num_records - 1) * 8       )

        pass


    """
    # Read a record with specified key
    """
    #Add column parameter
    def select(self, key, query_columns):
        recordList = []
        RID = self.index.keyToRID[key]
        (page, offset) = self.table.page_directory[RID]

        recordValues = []

        recordIndirection = self.readRecord(page, 0, offset)

        if(recordIndirection == 0):
            hasUpdated = False
        else:
            hasUpdated = True

        if(hasUpdated == False):
            j = 0
            for i in range(4,self.table.total_columns):
                if(query_columns[j] == 0):
                    recordValues.append(None)
                else:
                    recordValues.append(self.readRecord(page, i, offset))
                j = j + 1

            record = Record(RID,key, recordValues)
            recordList.append(record)
        else:
            j = 0
            for i in range(4,self.table.total_columns):
                if(query_columns[j] == 0):
                    recordValues.append(None)
                else:
                    (tailPage, tailOffset) = self.table.tail_page_directory[recordIndirection]

                    recordValues.append(self.readTailRecord(page, tailPage + i, tailOffset))

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


        recordIndirection = self.readRecord(page, 0, offset)
        if(recordIndirection == 0):
            hasUpdated = False
        else:
            hasUpdated = True



        i = 0
        while(self.table.tail_pages[page][i].num_records == 512):
            if(self.table.tail_pages[page][i].num_records == 512):
                i = i + self.table.total_columns
        current_page = i

        if(hasUpdated):
            tailIndirection = recordIndirection
        else:
            tailIndirection = 0


        self.table.tail_pages[page][i].write(tailIndirection)
        if (self.table.tail_pages[page][i].num_records == 512):
            new_page = Page()
            self.table.tail_pages[page].append(new_page)
        #
        # Need to change base page indirection and schema encoding
        #

        i = i + 1

        tailRID = self.tailRIDcount
        self.tailRIDcount = self.tailRIDcount - 1
        self.table.tail_pages[page][i].write(tailRID)
        if (self.table.tail_pages[page][i].num_records == 512):
            new_page = Page()
            self.table.tail_pages[page].append(new_page)
        i = i + 1

        # base page indirection = newly created tail page record RID
        self.table.pages[page][0].writeAtOffset(tailRID, offset)

        timeStamp = time()
        self.table.tail_pages[page][i].write(int(timeStamp))
        if (self.table.tail_pages[page][i].num_records == 512):
            new_page = Page()
            self.table.tail_pages[page].append(new_page)
        i = i + 1


        schema_encoding = self.readRecord(page, 3, offset)
        schema_page_index = i

        self.table.tail_pages[page][i].write(schema_encoding)
        if (self.table.tail_pages[page][i].num_records == 512):
            new_page = Page()
            self.table.tail_pages[page].append(new_page)
        i = i + 1

        change_in_schema = 0 # 000..000
        for column in columns:
                if(column != None):
                        self.table.tail_pages[page][i].write(column)
                        change_in_schema = 1
                if(column == None):
                    change_in_schema = change_in_schema << 1
                    if(tailIndirection == 0):
                        column_index = i % self.table.total_columns
                        basePageRecordColumn = self.readRecord(page, column_index, offset)
                        self.table.tail_pages[page][i].write(basePageRecordColumn)
                    if(tailIndirection != 0):
                        column_index = i % self.table.total_columns
                        (page_tail, offset_tail) = self.table.tail_page_directory[tailIndirection]
                        insert_value = self.readTailRecord(page, page_tail + column_index, offset_tail)
                        self.table.tail_pages[page][i].write(insert_value)



                if (self.table.tail_pages[page][i].num_records == 512):
                        new_page = Page()
                        self.table.tail_pages[page].append(new_page)
                i = i + 1

        schema_encoding = schema_encoding | change_in_schema
        self.table.tail_pages[page][schema_page_index].writeAtOffset(schema_encoding, (self.table.tail_pages[page][current_page].num_records - 1) * 8)
        self.table.pages[page][3].writeAtOffset(schema_encoding, offset)

        self.table.tail_page_directory[tailRID] = (current_page, (self.table.tail_pages[page][current_page].num_records - 1) * 8)

        pass

# it only reads from base pages
    def readRecord(self, page_range, page_index, offset):
        eightByteVal = ''
        for i in range(8):
            binaryByte = bin((self.table.pages[page_range][page_index].data[offset + i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8 - len(byte)) * '0' + byte
            eightByteVal = eightByteVal + byte;
        recordVal = int(eightByteVal, 2)

        return recordVal

    def readTailRecord(self, page_range, page_index, offset):
        eightByteVal = ''
        for i in range(8):
            binaryByte = bin((self.table.tail_pages[page_range][page_index].data[offset + i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8 - len(byte)) * '0' + byte
            eightByteVal = eightByteVal + byte;
        recordVal = int(eightByteVal, 2)

        return recordVal

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0

        for key in sorted(self.index.keyToRID.keys()):
                if(key < start_range or key > end_range):
                    continue
                RID = self.index.keyToRID[key]
                (page, offset) = self.table.page_directory[RID]

                if(self.readRecord(page,0,offset) == 0):
                    sum = sum + self.readRecord(page, aggregate_column_index + 4, offset)
                else:
                    tailIndirection = self.readRecord(page,0, offset)
                    (tail_page, tail_offset) = self.table.tail_page_directory[tailIndirection]
                    sum = sum + self.readTailRecord(page, tail_page + aggregate_column_index + 4, tail_offset)

        return sum
        pass


    def write_to_disk(self):
        for i in range(0, len(self.table.pages)):
            name = self.table.name + "_" + str(i)
            file = open(name, "wb+")
            for j in range(0, self.table.total_columns):
                file.write(self.table.pages[i][j].data)



