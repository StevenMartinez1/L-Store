from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if (self.num_records < 512):
            return true
        else:
            return false
        pass

    def write(self, value):
        self.num_records += 1
        offset = (self.num_records - 1) * 8


        for k in range(0, 8):
            eight_bits = 255
            if(value == None):
                break
            eight_bits = value & eight_bits
            self.data[offset + 7 - k] = eight_bits
            value = value >> 8


        pass


    def writeAtOffset(self, value, offset):
        #self.num_records += 1

        for k in range(0, 8):
            eight_bits = 255
            eight_bits = value & eight_bits
            self.data[offset + 7 - k] = eight_bits
            value = value >> 8
        pass
