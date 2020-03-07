from template.table import Table, Record
from template.index import Index
from template.page import *
from template.config import *
from time import *
import threading


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """
    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        self.RIDcount = 1
        self.tailRIDcount = 2 ** 64 - 1
        self.lock = threading.Lock()

    """
    # internal Method
    # Read a record with specified RID
    """
    def delete(self, key):

        self.updateRID = self.table.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]
        recordIndirection = self.readRecord(page, 0, offset)

        if (recordIndirection == 0):
            hasUpdated = False
        else:
            TSP = self.readRecord(page, 4, 0)
            if (TSP == 0):
                hasUpdated = True
            else:
                if (recordIndirection < TSP):
                    hasUpdated = True
                else:
                    hasUpdated = False

        # delete from tail page table
        if(hasUpdated):
            while(recordIndirection != 0):
                (page2, offset2) = self.table.tail_page_directory[recordIndirection]
                recordIndirection = self.readTailRecord(page,page2, offset2)
                for l in range(0, self.table.total_columns):
                    self.table.tail_pages[page][l].writeAtOffset(0, offset2)

        # delete from base pages table
        for i in range(0, self.table.total_columns):
            self.table.pages[page][i].writeAtOffset(0, offset)

        del self.table.keyToRID[key]

    """
    # writes the page ranges to disk
    """
    def write_range_to_disc(self, pageRange, rangeID):
        name = self.table.name +"_" + str(rangeID) # Giving the new files their names
        f = open(name, "wb+")

        for j in range(0, self.table.total_columns):
            f.write(self.table.pages[pageRange][j].data)
            number_of_records = self.table.pages[pageRange][j].num_records
            f.write(number_of_records.to_bytes(8, 'little'))

        for k in range(0, len(self.table.tail_pages[pageRange])):
            f.write(self.table.tail_pages[pageRange][k].data)
            number_of_records = self.table.tail_pages[pageRange][k].num_records
            f.write(number_of_records.to_bytes(8, 'little'))

        f.close()

    """
    # Insert a record with specified columns
    """
    def insert(self, *columns):

        schema_encoding = 0
        i = 0
        k = 0
        max_range_reached = False # means we dont need to create new page ranges
        isFull = False # means bufferpool is full

        while(self.table.pages[i][k].num_records == 512):
                i = i + 1
                if(i == 10):
                    max_range_reached = True
                    if(self.table.pages[i][k].num_records == 512):
                        isFull = True
                        break
                if(isFull == True and i != 10):
                    if(self.table.pages[i][k].num_records == 512):
                        isFull = True
                        break

        if(isFull == True):
            range_to_evict = 0
            for p in range(0, len(self.table.pages)):
                if(self.table.use_count[p] == min(self.table.use_count)):
                    range_to_evict = p
            self.table.use_count[range_to_evict] == 0
            while(self.table.pin[range_to_evict] != 0): # Is this line necessary?
                continue

            isDirty = False
            if(self.table.clean[range_to_evict] == 1):
                isDirty = True
            if(isDirty):
                self.write_range_to_disc(range_to_evict, self.table.ranges_in_buffer[range_to_evict])
            self.table.clean[range_to_evict] == 0

            #delete the range and replace it with empty range and save index
            del self.table.pages[range_to_evict]
            del self.table.tail_pages[range_to_evict]

            self.empty_base_page_range = []
            self.empty_tail_page_range = []

            for v in range (0, self.table.total_columns):
                new_page_1 = Page()
                new_page_2 = Page()
                self.empty_base_page_range.insert(v,new_page_1)
                self.empty_tail_page_range.insert(v,new_page_2)

            self.table.pages.insert(range_to_evict, self.empty_base_page_range)
            self.table.tail_pages.insert(range_to_evict, self.empty_tail_page_range)

            self.table.ranges_in_buffer[range_to_evict] = (self.table.range_id)
            self.table.range_id = self.table.range_id + 1

            i = range_to_evict
            isFull = False

        self.table.pin[i] += 1
        current_page = i

        if(self.table.pages[i][k].num_records == 0):
            # insert TSP for all pages of the range as first record
            TSP = 0
            for w in range (0, self.table.total_columns):
                self.table.pages[i][w].write(TSP)

        indirection = 0
        self.table.pages[i][k].write(indirection)
        if (self.table.pages[i][k].num_records == 512 and max_range_reached == False):
            new_page = Page()
            new_tail_page = Page()

            self.table.ranges_in_buffer.append(self.table.range_id)
            self.table.range_id = self.table.range_id + 1

            self.table.pages.append([new_page])
            self.table.tail_pages.append([new_tail_page])

        k = k + 1
        RID = self.RIDcount
        self.RIDcount = self.RIDcount + 1
        self.table.pages[i][k].write(RID)

        if ((self.table.pages[i][k].num_records == 512) and max_range_reached == False):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages[i + 1].append(new_page)
            self.table.tail_pages[i + 1].append(new_tail_page)

        k = k + 1
        timeStamp = time()
        self.table.pages[i][k].write(int(timeStamp))

        if (self.table.pages[i][k].num_records == 512 and max_range_reached == False):
            new_page = Page()
            new_tail_page = Page()
            self.table.pages[i + 1].append(new_page)
            self.table.tail_pages[i + 1].append(new_tail_page)

        k = k + 1
        self.table.pages[i][k].write(schema_encoding)

        if (self.table.pages[i][k].num_records == 512 and max_range_reached == False):
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

                if (self.table.pages[i][k].num_records == 512 and max_range_reached == False):
                        new_page = Page()
                        new_tail_page = Page()
                        self.table.pages[i + 1].append(new_page)
                        self.table.tail_pages[i + 1].append(new_tail_page)

                k = k + 1
                j = j + 1
    
        self.table.keyToRID[key] = RID
        self.table.page_directory[RID] =(current_page, (self.table.pages[current_page][0].num_records - 1) * 8)

        self.table.clean[current_page] = 1
        self.table.pin[current_page] -= 1

    """
    # writing each table page to disk
    """
    def write_db_to_disc(self):
        for i in range(0, len(self.table.pages)):
            self.write_to_disc(i)

    """
    # writing each page to disk
    """
    def write_to_disc(self, pageRange):
        name = self.table.name +"_" + str(pageRange)
        f = open(name, "wb+")

        for j in range(0, self.table.total_columns):
            f.write(self.table.pages[pageRange][j].data)
        for k in range(0, len(self.table.tail_pages[pageRange])):
            f.write(self.table.tail_pages[pageRange][k])

        f.close()

    """
    # reading the page range from disk
    """
    def read_range_from_disc(self, pageRange):
        name = self.table.name +"_" + str(pageRange)
        f = open(name, "rb+")
        f.seek(0,0)
        base_page = Page()
        base_page_2 = Page()
        base_range = [base_page]
        tail_range = [base_page_2]

        base_range[0].data = bytearray(f.read(4096))
        num_rec = f.read(8)# maybe add bytearray syntax
        base_range[0].num_records = int.from_bytes(num_rec, "little")

        for j in range(1, self.table.total_columns):
            base_page = Page()
            base_range.append(base_page)
            base_range[j].data = bytearray(f.read(4096))
            num_rec = f.read(8) # maybe needs bytearray
            base_range[j].num_records = int.from_bytes(num_rec, "little")

        tail_range[0].data = bytearray(f.read(4096))
        num_rec = f.read(8) # maybe needs bytearray
        tail_range[0].num_records = int.from_bytes(num_rec, "little")

        for k in range(1, self.table.total_columns):
            tail_page = Page()
            tail_range.append(tail_page)
            tail_range[k].data = bytearray(f.read(4096))
            num_rec = f.read(8)
            tail_range[k].num_records = int.from_bytes(num_rec, "little")

        f.close()
        return base_range, tail_range

    """
    # writing the metadata to disk
    """
    def write_metadata(self):
        name = "Metadata" + "_" + self.table.name
        f = open(name, "w+")
        f.write(self.table.total_columns)
        f.close

    """
    # reading pagerange from disk
    """
    def read_from_disc(self, pageRange):
        name = self.table.name + "_" + str(pageRange)
        f = open(name, "rb")
        fileContents = f.read()
        self.table.pages[pageRange][0].data = f.seek(0)

        for j in range(1, self.table.total_columns):
            self.table.pages[pageRange][j].data = f.seek(4096,1)

        self.table.tail_pages[pageRange][0].data = f.seek(0,1)

        for k in range(1, len(self.table.tail_pages[pageRange])):
            self.table.pages[pageRange][k].data = f.seek(4096, 1)

        f.close()

    """
    # returns the specified set of columns from the record with the given key
    """
    def select(self, key, column, query_columns):
        column = 0
        recordList = []
        RID = self.table.keyToRID[key]
        (page, offset) = self.find_the_page(key)
        self.table.pin[page] += 1
        recordValues = []
        recordIndirection = self.readRecord(page, 0, offset)

        if(recordIndirection == 0):
            hasUpdated = False
        else:
            TSP = self.readRecord(page, 4, 0)
            if(TSP == 0):
                hasUpdated = True
            else:
                if(recordIndirection < TSP):
                    hasUpdated = True
                else:
                    hasUpdated = False

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

        self.table.pin[page] -= 1
        return recordList

    """
    # finding a page in a page range
    """
    def find_the_page(self, key):
        self.updateRID = self.table.keyToRID[key]
        (page, offset) = self.table.page_directory[self.updateRID]

        # find the unique Range of the key using rid
        self.unique_range_of_record = (self.updateRID - 1) // 511

        #check if this range in buffer
        self.here = False

        for i in range(0, len(self.table.ranges_in_buffer)):
            if(self.unique_range_of_record == self.table.ranges_in_buffer[i]):
                self.here = True
                page = i
                break

        if(self.here == False):
            range_to_evict = 0

            for p in range(0, len(self.table.pages)):
                if (self.table.use_count[p] == min(self.table.use_count)):
                    range_to_evict = p

            self.table.use_count[range_to_evict] = 0

            while (self.table.pin[range_to_evict] != 0):
                continue

            isDirty = False

            if (self.table.clean[range_to_evict] == 1):
                isDirty = True

            if (isDirty):
                self.write_range_to_disc(range_to_evict, self.table.ranges_in_buffer[range_to_evict])

            self.table.clean[range_to_evict] = 0

            # delete the range and replace it with empty range and save index
            del self.table.pages[range_to_evict]
            del self.table.tail_pages[range_to_evict]

            self.base_list, self.tail_list = self.read_range_from_disc(self.unique_range_of_record)
            self.table.pages.insert(range_to_evict, self.base_list)
            self.table.tail_pages.insert(range_to_evict, self.tail_list)
            self.table.ranges_in_buffer[range_to_evict] = (self.unique_range_of_record)
            page = range_to_evict

        return(page, offset)

    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        baseRID = self.table.keyToRID[key]
        (page, offset) = self.find_the_page(key)
        self.table.pin[page] += 1
        recordIndirection = self.readRecord(page, 0, offset)

        if(recordIndirection == 0):
            hasUpdated = False
        else:
            TSP = self.readRecord(page, 4, 0)
            if(TSP == 0):
                hasUpdated = True
            else:
                if(recordIndirection < TSP):
                    hasUpdated = True
                else:
                    hasUpdated = False
        i = 0

        while(self.table.tail_pages[page][i].num_records == 512):
                i = i + self.table.total_columns

        current_page = i

        if(self.table.tail_pages[page][i].num_records == 0):
            # insert TSP for all pages of the range as first record
            TSP = 0
            i_temp = i

            for w in range (0, self.table.total_columns):
                self.table.tail_pages[page][i_temp].write(TSP)
                i_temp = i_temp + 1

        if(hasUpdated):
            tailIndirection = recordIndirection
        else:
            tailIndirection = 0

        self.table.tail_pages[page][i].write(tailIndirection)

        if (self.table.tail_pages[page][i].num_records == 512):
            new_page = Page()
            self.table.tail_pages[page].append(new_page)

        # Need to change base page indirection and schema encoding
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
        self.table.tail_pages[page][i].write(baseRID)

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

                    if(tailIndirection != 0 and hasUpdated == True):
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
        self.table.use_count[page] += 1
        self.table.clean[page] = 1
        self.table.pin[page] -= 1

        if (self.table.tail_pages[page][0].num_records == 512): # it means first set of tail page range is full
            self.merge(page)

    """
    # After tail pages are full, merges in the background thread
    """
    def merge(self, page_range):
        # 1. create the copy of base page range
        self.copy_pages = []

        for page_index in range(4, self.table.total_columns):
            #new_page = Page()
            new_page = self.table.pages[page_range][page_index]
            self.copy_pages.append(new_page)

        # 2. update it
        #      2.1 read RID of last record in the tail page range
        record_num = 512
        offset_of_last_rec = (record_num - 1) * 8
        last_RID = self.readTailRecord(page_range, 1, offset_of_last_rec)
        counter = 0

        # insert RID as first record to all base pages
        for page_index in range(4, self.table.total_columns):
            self.copy_pages[counter].writeAtOffset(last_RID, 0)
            counter = counter + 1

        while(record_num != 1):
            if(last_RID == 0):
                record_num = record_num - 1
                offset_of_last_rec = (record_num - 1) * 8
                last_RID = self.readTailRecord(page_range, 1, offset_of_last_rec)
            else:
                # insert all tail cols into new base page range cols
                counter = 0

                for page_index in range(4, self.table.total_columns):
                    tail_rec_to_insert = self.readTailRecord(page_range, page_index, offset_of_last_rec)
                    base_rid_of_tail_rec = self.readTailRecord(page_range, 2, offset_of_last_rec)
                    (base_page_1, base_offset_1) = self.table.page_directory[base_rid_of_tail_rec]
                    self.copy_pages[counter].writeAtOffset(tail_rec_to_insert, base_offset_1)
                    counter = counter + 1

                inderection = self.readTailRecord(page_range, 0, offset_of_last_rec)

                while(inderection != 0):
                    (tail_page, tail_offset) = self.table.tail_page_directory[inderection]
                    self.table.tail_pages[page_range][1].writeAtOffset(0, tail_offset)
                    inderection = self.readTailRecord(page_range, 0, tail_offset)

                record_num = record_num - 1
                offset_of_last_rec = (record_num - 1) * 8
                last_RID = self.readTailRecord(page_range, 1, offset_of_last_rec)

        # 3. replace the base page range
        self.lock.acquire()
        counter = 0

        for i in range(4, self.table.total_columns):
            page_to_insert = self.copy_pages[counter]
            self.table.pages[page_range].append(page_to_insert) #insert new base page
            del self.table.pages[page_range][4] # delete the old base page
            counter = counter + 1
        # 4. lock page directory, change it and unlock
        # self.lock.acquire()

        # self.lock.release()
        # 5. delete tail pages
        for i in range(0, self.table.total_columns):
            del self.table.tail_pages[page_range][0]
        self.lock.release()
        # 6. update the function depending on the inderection

    """
    # read a record from a base page
    """
    def readRecord(self, page_range, page_index, offset):
        eightByteVal = ''

        for i in range(8):
            binaryByte = bin((self.table.pages[page_range][page_index].data[offset + i]))
            byte = binaryByte[2:len(binaryByte)]
            byte = (8 - len(byte)) * '0' + byte
            eightByteVal = eightByteVal + byte;

        recordVal = int(eightByteVal, 2)
        return recordVal

    """
    # reads the tail record from a tail page
    """
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
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0

        for key in sorted(self.table.keyToRID.keys()):
                if(key < start_range or key > end_range):
                    continue

                RID = self.table.keyToRID[key]
                (page, offset) = self.find_the_page(key)
                self.table.pin[page] += 1
                recordIndirection = self.readRecord(page, 0, offset)

                if (recordIndirection == 0):
                    hasUpdated = False
                else:
                    TSP = self.readRecord(page, 4, 0)
                    if (TSP == 0):
                        hasUpdated = True
                    else:
                        if (recordIndirection < TSP):
                            hasUpdated = True
                        else:
                            hasUpdated = False

                if(hasUpdated == False):
                    sum = sum + self.readRecord(page, aggregate_column_index + 4, offset)
                else:
                    tailIndirection = self.readRecord(page,0, offset)
                    (tail_page, tail_offset) = self.table.tail_page_directory[tailIndirection]
                    sum = sum + self.readTailRecord(page, tail_page + aggregate_column_index + 4, tail_offset)

                self.table.pin[page] -= 1

        return sum
