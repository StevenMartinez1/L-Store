from template.table import Table
from template.query import *
from template.index import *
import pickle

class Database():

    def __init__(self):
        self.tables = []

        pass


    def write_db_to_disc(self):
        for i in range(0, len(self.tables)):
            self.write_table_to_disc(i)
        self.write_metadata()
    def write_table_to_disc(self, table_index):
        for range_index in range(0, len(self.tables[table_index].pages)):
            range_id = self.tables[table_index].ranges_in_buffer[range_index]
            self.write_range_to_disc(range_index, range_id, table_index)


    def write_range_to_disc(self, pageRange, rangeID, table_index):
        name = self.tables[table_index].name +"_" + str(rangeID)
        f = open(name, "wb+")
        for j in range(0, self.tables[table_index].total_columns):
            f.write(self.tables[table_index].pages[pageRange][j].data)
            number_of_records = self.tables[table_index].pages[pageRange][j].num_records
            f.write(number_of_records.to_bytes(8, 'little'))

        for k in range(0, len(self.tables[table_index].tail_pages[pageRange])):
            f.write(self.tables[table_index].tail_pages[pageRange][k].data)
            number_of_records = self.tables[table_index].tail_pages[pageRange][k].num_records
            f.write(number_of_records.to_bytes(8, 'little'))
        f.close()




    def write_metadata(self):
        with open('Database.txt', 'wb') as handle:
            pickle.dump(self.tables, handle)



    def open(self, path):
        with open('Database.txt', 'rb') as handle:
            self.tables = pickle.loads(handle.read())


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
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                return self.tables[i]
        pass