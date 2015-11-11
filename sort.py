from __future__ import print_function
import sys
import os
import datetime
import argparse

from pyspark import SparkContext
# from pyspark.conf import SparkConf

BASENAME_TIMESTAMP_PATTERN = 'pagecounts-%Y%m%d-%H%M%S.gz'


def input_mapper(filename):
    basename = os.path.basename(filename)
    timestamp = datetime.datetime.strptime(
        basename, BASENAME_TIMESTAMP_PATTERN)

    def line_mapper(line):
        project, page, counts, _ = line.split(u' ')

        counts = int(counts)
        project = project.lower()

        return (timestamp, project, page, counts)

    return line_mapper


def input_line_filter_provider(project_list):
    def line_filter(line):
        timestamp, project, page, counts = line

        return project in project_list

    return line_filter


def line_sorting_key(line):
    timestamp, project, page, counts = line
    return (project, page, timestamp)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Sort wikimedia pagecounts by (project, article)',
    )
    parser.add_argument(
        'input_files',
        metavar='INPUT_FILE',
        nargs='+',
        help='''\
Input file in gzip format. Basename must match the pattern:
    {pattern}'''.format(pattern=BASENAME_TIMESTAMP_PATTERN),
    )
    parser.add_argument(
        'output_file',
        metavar='OUTPUT_FILE',
        help="Output file (it will be gzip'd)",
    )
    parser.add_argument(
        '--input-partitions',
        type=int,
        required=False,
        help='''\
Number of input partitions [default: SparkContext.defaultMinPartitions]''',
    )
    parser.add_argument(
        '--output-partitions',
        type=int,
        required=False,
        help='Number of output partitions [default: input-partitions]',
    )
    parser.add_argument(
        '--projects', '-p',
        type=unicode,
        default=None,
        required=False,
        help='Comma-separated list of projects to keep. Leave empty for all.',
    )

    args = parser.parse_args()

    return args


def line_tuple_to_text(line):
    timestamp, project, page, counts = line

    output_line = u'{project} {page} {timestamp} {counts}\n'.format(
        project=project,
        page=page,
        timestamp=timestamp.strftime('%Y%m%d-%H%M%S'),
        counts=counts,
    ).encode(encoding='utf-8')

    return output_line


def main():
    args = parse_arguments()
    print(args)

    if os.path.exists(args.output_file):
        print('Output file already exists:', args.output_file)
        sys.exit(1)

    sc = SparkContext()

    input_partitions = args.input_partitions or sc.defaultMinPartitions
    output_partitions = args.output_partitions or input_partitions

    rdds_list = [sc.textFile(
        name=input_file,
        minPartitions=input_partitions,
        use_unicode=True,
        ).map(input_mapper(input_file)) for input_file in args.input_files]

    union_rdd = sc.union(rdds_list)

    if args.projects:
        projects = args.projects.split(u',')
        line_filter = input_line_filter_provider(projects)
        filtered_rdd = union_rdd.filter(line_filter)
    else:
        filtered_rdd = union_rdd

    sorted_rdd = filtered_rdd.sortBy(
        keyfunc=line_sorting_key,
        ascending=True,
        numPartitions=output_partitions,
        )

    sorted_rdd_text = sorted_rdd.map(line_tuple_to_text)

    sorted_rdd_text.saveAsTextFile(
        args.output_file,
        compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec',
    )

if __name__ == '__main__':
    main()
