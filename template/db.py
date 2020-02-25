from template.table import Table
from template.query import *

class Database():

    def __init__(self):
        self.tables = []

        pass

    #writes the entire database to disk
    def write_db_to_disc(self):
        #Database is made up of tables: so write the tables to disk
        for i in range(0, len(self.tables)):
            #write a table to disk
            self.write_table_to_disc(i)

    #Given a table write the table to disk
    def write_table_to_disc(self, table_index):
        #Table is made up of Page ranges: write page ranges to disk
        for i in range(0, len(self.tables[table_index].pages)):
            #write a single page range to disk
            self.write_range_to_disc(i,table_index)
        #Write the metadata of a table to disk
        self.write_metadata(table_index)
    
    """
    given a page range write the page range to disk
    1 Page Range == however pany columns we have
    when we are writing a columns contents to file it is only one page worth (500 records)
    """
    def write_range_to_disc(self, pageRange, table_index):
        #compose the file name for the page range we are opening. 
        name = self.tables[table_index].name +"_" + str(pageRange)
        #open the page range file
        f = open(name, "wb+")

        #For every column of the table write the contents of that column to file
        for j in range(0, self.tables[table_index].total_columns):
            f.write(self.tables[table_index].pages[pageRange][j].data)

        #For every tail page we have, write the page to file
        for k in range(0, len(self.tables[table_index].tail_pages[pageRange])):
            f.write(self.tables[table_index].tail_pages[pageRange][k].data)
        f.close()

    #create file for recording the Metadata of a table
    def write_metadata(self, table_index):
        #make the file name
        name = "Metadata" + "_" + self.tables[table_index].name
        #open the metadata file, if it doesnt exist then create it
        f = open(name, "w+")
        #so far this justs writes the amount of columns a table has
        f.write(str(self.tables[table_index].total_columns))
        #close the Metadata file
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

        #Steven: Added this in
        if(len(self.tables[0].tail_pages[pageRange][0].data) != 4096):
            self.tables[0].tail_pages[pageRange][0].data = bytearray(4096)

        for k in range(1, len(self.tables[table_index].tail_pages[pageRange])):
            f.seek(4096, 1)
            self.data = bytearray(4096)
            self.data = bytearray(f.read(4096))

            #Steven: Added this in
            if(len(self.data) != 4096):
                self.data = bytearray(4096)
            
            self.tables[table_index].tail_pages[pageRange][k].data = self.data
        f.close()


    def open(self):
        self.read_db_from_disc()
        #this function will require more details later
        pass

    def close(self):
        self.write_db_to_disc()
        #this function will require more details later
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