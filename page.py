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
        num1 = (self.num_records - 1) * 8


        for k in range(0, 7):
            eight_bits = 255
            eight_bits = value & eight_bits
            self.data[num1 + 7 - k] = eight_bits
            value = value >> 8


        pass

