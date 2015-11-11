from __future__ import print_function
import sys
import os
import datetime
import glob

from pyspark import SparkContext
# from pyspark.conf import SparkConf

USAGE = '''\
pyTeraSort.py <input directory> <output directory> [options]

Options:
-inputPartitions  <int> number of input partitions
-outputPartitions <int> number of output partitions'''


def input_mapper(filename):
    basename = os.path.basename(filename)
    timestamp = datetime.datetime.strptime(
        basename, 'pagecounts-%Y%m%d-%H%M%S.gz')

    def line_mapper(line):
        # line_unicode = line.decode(encoding='ISO-8859-2')
        line_unicode = line
        project, page, counts, _ = line_unicode.split(' ')

        counts = int(counts)
        project = project.lower()
        # print(project, page, counts)
        return (timestamp, project, page, counts)

    return line_mapper


def input_line_filter(line):
    # print('input_line_filter: line: ', line)
    timestamp, project, page, counts = line

    return project.lower() == u'en'


def line_tuple_to_text(line):
    timestamp, project, page, counts = line

    output_line = u'{project} {page} {timestamp} {counts}\n'.format(
        project=project,
        page=page,
        timestamp=timestamp.strftime('%Y%m%d-%H%M%S'),
        counts=counts,
    ).encode(encoding='utf-8')

    return output_line


def main(args):
    args = sys.argv[1:]
    # Process command line args
    if len(args) >= 2:
        pass
    else:
        print("no input file specified and or output")
        print(USAGE)
        sys.exit()

    files = glob.glob(args[0] + '/*.gz')
    print(files)

    # Create Spark Contex for NONE local MODE
    sc = SparkContext()
    if '-inputPartition' in args:
        inp = int(args[args.index('-inputPartition') + 1])
    else:
        inp = sc.defaultMinPartitions

    if '-outputPartition' in args:
        onp = int(args[args.index('-outputPartition') + 1])
    else:
        onp = inp

    rdds = [sc.textFile(
        name=input_file,
        minPartitions=inp,
        use_unicode=True,
        ).map(input_mapper(input_file)) for input_file in files]

    rdd = sc.union(rdds)

    filtered_rdd = rdd.filter(input_line_filter)

    ordd = filtered_rdd.sortBy(
        keyfunc=lambda t: (t[1], t[2]),
        ascending=True,
        numPartitions=onp,
        )

    mapped_ordd = ordd.map(line_tuple_to_text)
    mapped_ordd.saveAsTextFile(args[1] + '/output.gz',
        compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

if __name__ == '__main__':
    main()
