import sys
import os
from pyspark import SparkContext
from pyspark.conf import SparkConf
from datetime import datetime


def main(argList):	
	# Process command line args
	if len(argList) >= 1:
		pass
	else:
		print ("no input file specified or prefix")
		usage()
		sys.exit()
		
	if '-inputPartition' in argList:
		inp = int(argList[argList.index('-inputPartition') + 1])
	else:
		inp = 1

	if '-outputPartition' in argList:
		onp = int(argList[argList.index('-outputPartition') + 1])
	else:
		onp = inp
		
	
	# Create Spark Contex for NONE local MODE
	sc = SparkContext(conf=SparkConf()) 
	
	
	irdd = sc.textFile(argList[0], inp).map(lambda x: (x[0:10],x[10:]))
	ordd = irdd.sortByKey(True, onp)
	ordd.saveAsTextFile('output')
	
def usage():
		print 'pyTeraSort.py <input file or directory> -options'
		print '-inputPartitions <int> number of input partitions'
		print '-outputPartitions <int> number of output partitions'
		return
if __name__ == '__main__':
	main(sys.argv[1:])
