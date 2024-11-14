from mrjob.job import MRJob

class Two_Dimensional(MRJob):

    def mapper(self, _, line):
        try:
            column, row, value = line.split(',')
            value = int(value)
            yield column, {'value_type': 'col','value': value, 'value_location': row}  # Yield column data
            yield row, {'value_type': 'row','value': value, 'value_location': column}      # Yield row data
        except ValueError:
            pass  # Skip malformed lines

    def reducer(self, key, values):
        max_value = None
        min_value = None
        row_locations = []
        column_locations = []
        
        for value in values:
            if value['value_type'] == 'col':
                if max_value is None or value['value'] > max_value:
                    max_value = value['value']
                    row_locations = [value['value_location']]  # Reset to new max location
                elif value['value'] == max_value:
                    row_locations.append(value['value_location'])
                    
            elif value['value_type'] == 'row':
                if min_value is None or value['value'] < min_value:
                    min_value = value['value']
                    column_locations = [value['value_location']]  # Reset to new min location
                elif value['value'] == min_value:
                    column_locations.append(value['value_location'])
                    
        if key[0] in 'ABCDEFGHIJ':
            yield key, {'value': max_value, 'row_location':row_locations}
        else:  # Rows should be K-T
            yield key, {'value':min_value, 'column_location': column_locations}

if __name__ == '__main__':
    Two_Dimensional.run()
