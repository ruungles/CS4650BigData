
import re
import json
from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class WindDirection(MRJob):

    def mapper(self, _, line):
        val = line.strip()
        (wind_direct, wind_q, temp, temp_q) = (val[60:63],val[63:64], val[87:92], val[92:93])
        if (temp != "+9999" and re.match(QUALITY_RE,temp_q)):
            if(wind_direct != "999" and re.match(QUALITY_RE,wind_q)):
                yield wind_direct, {'temperature': int(temp), "count": 1}

    def reducer (self, key, values):
        count = 0
        max_temp = float('-inf')
        min_temp = float('inf')
        for x in values:
            if x["temperature"]> max_temp:
                max_temp = x["temperature"]
            if x["temperature"]< min_temp:
                min_temp = x["temperature"]
            count+= x ['count']
        yield key, {"low": min_temp, "high": max_temp, "count": count}


if __name__ == '__main__':
    WindDirection.run()
