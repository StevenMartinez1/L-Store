from template.table import Table
from template.query import *

class Database():

    def __init__(self):
        self.tables = []

        pass


    def write_db_to_disc(self):
        for i in range(0, len(self.tables)):
            self.write_table_to_disc(i)
    def write_table_to_disc(self, table_index):
        for i in range(0, len(self.tables[table_index].pages)):
            self.write_range_to_disc(i,table_index)
        self.write_metadata(table_index)
    def write_range_to_disc(self, pageRange, table_index):
        name = self.tables[table_index].name +"_" + str(pageRange)
        f = open(name, "wb+")
        for j in range(0, self.tables[table_index].total_columns):
            f.write(self.tables[table_index].pages[pageRange][j].data)
        for k in range(0, len(self.tables[table_index].tail_pages[pageRange])):
            f.write(self.tables[table_index].tail_pages[pageRange][k].data)
        f.close()

    def write_metadata(self, table_index):
        name = "Metadata" + "_" + self.tables[table_index].name
        f = open(name, "w+")
        f.write(str(self.tables[table_index].total_columns))
        f.close

    def read_db_from_disc(self):
        for i in range(0, len(self.tables)):
            self.read_table_from_disc(i)
    def read_table_from_disc(self, table_index):
        for i in range(0, len(self.tables[table_index].pages)):
            self.read_range_from_disc(i, table_index)
    def read_range_from_disc(self, pageRange, table_index):
        name = self.tables[table_index].name +"_" + str(pageRange)
        f = open(name, "rb+")
        #fileContents = f.read()
        f.seek(0,0)
        self.tables[table_index].pages[pageRange][0].data = bytearray(f.read(4096))
        for j in range(1, self.tables[table_index].total_columns):
            f.seek(4096, 1)
            self.data = bytearray(4096)
            self.data = bytearray(f.read(4096))

            self.tables[table_index].pages[pageRange][j].data = self.data
        f.seek(4096, 1)
        self.tables[0].tail_pages[pageRange][0].data = bytearray(f.read(4096))

        #Added this in
        if(len(self.tables[0].tail_pages[pageRange][0].data) != 4096):
            self.tables[0].tail_pages[pageRange][0].data = bytearray(4096)

        for k in range(1, len(self.tables[table_index].tail_pages[pageRange])):
            f.seek(4096, 1)
            self.data = bytearray(4096)
            self.data = bytearray(f.read(4096))

            #Added this in
            if(len(self.data) != 4096):
                self.data = bytearray(4096)
            
            self.tables[table_index].tail_pages[pageRange][k].data = self.data
        f.close()

    def open(self):
        self.read_db_from_disc()
        pass

    def close(self):
        self.write_db_to_disc()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass