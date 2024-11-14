from mrjob.job import MRJob

class Two_Dimensional(MRJob):

    def mapper(self, _, line):
        try:
            column, row, value = line.split(',')
            value = int(value)
            yield column, ('col', value)  # Yield column data
            yield row, ('row', value)      # Yield row data
        except ValueError:
            pass  # Skip malformed lines

    def reducer(self, key, values):
        max_value = None
        min_value = None
        
        for value_type, value in values:
            if value_type == 'col':
                if max_value is None or value > max_value:
                    max_value = value
            elif value_type == 'row':
                if min_value is None or value < min_value:
                    min_value = value
                    
        if key[0] in 'ABCDEFGHIJ':
            yield key, max_value
        else:  # Rows should be K-T
            yield key, min_value

if __name__ == '__main__':
    Two_Dimensional.run()

