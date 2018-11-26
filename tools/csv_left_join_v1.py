import csv
import json
import os
import ntpath
import uuid
import sys

import logging

log = logging.getLogger(__name__)

# Usage: python csv_left_join.py first_csv second_csv first_column_name second_column_name


# 1. load first_csv
# 2. create map of first_csv
# 3. load second_csv
# 4. join those csv


def load_csv_as_keyvp(csv_file_name, mapping_column_name):
    content = {}
    total_rows = 0

    with open(csv_file_name, 'rb') as csvfile_handle:
        csv_reader = csv.reader(csvfile_handle)

        headers = None

        for row in csv_reader:

            total_rows = total_rows + 1
            if headers is None:
                headers = list(row)
            else:
                key_value = row[headers.index(mapping_column_name)]
                content[key_value] = list(row)

    log.info("Total number of rows loaded from file: %s are: %d", csv_file_name, total_rows)

    return content, headers


def setup_defaults():
    log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    logger_file_full_path = os.path.join("./", "plog-%s.log" % ntpath.basename(__file__))
    file_handler = logging.FileHandler(logger_file_full_path)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)


def left_join(first_content, first_headers, second_content, second_headers, resultant_file_name):

    all_headers = list(first_headers)
    all_headers.extend(second_headers)

    log.info("Merging into file: %s", resultant_file_name)
    with open(resultant_file_name, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(all_headers)

        for key in first_content:
            buffer = []
            buffer.extend(first_content[key])

            if key in second_content:

                buffer.extend(second_content[key])
            else:

                for _ in second_headers:
                    buffer.append("")

            writer.writerow(buffer)

    log.info("Completed writing to file: %s", resultant_file_name)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python csv_left_join.py first_file second_file csv_join_columns")
        sys.exit(1)

    setup_defaults()

    first_file = sys.argv[1]
    second_file = sys.argv[2]
    join_columns = sys.argv[3]

    resultant_file_name = "%s-resultant-merged.csv" % str(uuid.uuid4())

    first_content, first_headers = load_csv_as_keyvp(first_file, join_columns)
    # print(json.dumps(first_content, indent=4))

    second_content, second_headers = load_csv_as_keyvp(second_file, join_columns)
    # print(json.dumps(second_content, indent=4))

    if len(first_content) < 2:
        log.info("First file is empty: %s", first_file)

    if len(second_content) < 2:
        log.info("Second file is empty: %s", second_file)

    left_join(first_content, first_headers, second_content, second_headers, resultant_file_name)
