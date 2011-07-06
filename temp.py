print [500, 1000, 5000]+[10000*i for i in range(21)]
from datetime import timedelta
print [timedelta(seconds=i*10*60) for i in range(1,19)]
import pprint
print pprint.pformat({'3':19, '2':23})