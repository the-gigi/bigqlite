import functools
import glob
import multiprocessing
import os
import csv
import shutil
import sqlite3

import sh


# def split_csv(filename: str, max_lines: int, output_dir='.', with_header=True):
#     """Split a large csv file to multiple files where each file contains at most `max_lines` lines
#
#     """
#     if not os.path.exists(filename):
#         raise FileNotFoundError(f"File '{filename}' not found.")
#
#     if max_lines <= 0:
#         raise ValueError("max_lines must be greater than 0.")
#
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir, exist_ok=True)
#
#     # Open the input CSV file for reading
#     with open(filename, 'r', newline='') as input_file:
#         reader = csv.reader(input_file)
#
#         # Initialize variables for the output files
#         current_output_file = open(f"{output_dir}/output_1.csv", 'w')
#         output_files = [current_output_file]
#         writer = csv.writer(current_output_file)
#         current_line_count = 0
#         header = ''
#         if with_header:
#             header = next(reader)
#             writer.writerow(header)
#
#         # Read and distribute rows to output files
#         for row in reader:
#             if current_line_count >= max_lines:
#                 current_line_count = 0
#                 current_output_file = open(f"{output_dir}/output_{len(output_files) + 1}.csv", 'w', newline='')
#                 writer = csv.writer(current_output_file)
#                 if with_header:
#                     writer.writerow(header)
#                 output_files.append(current_output_file)
#
#             writer.writerow(row)
#             current_line_count += 1
#
#         # Close all output files
#         for file in output_files:
#             file.close()


# def process_csv(filename: str, process_func, result_func, has_header=True):
#     """The process_csv function takes a csv file, a process_func function and result_func function
#
#     It reads the csv lines one by one and passes them to the process_func
#
#     If the process_func returns result that is not None it passes it to the result_func
#
#     If there is a header it skips it
#     """
#     with open(filename, 'r', newline='') as csv_file:
#         reader = csv.reader(csv_file)
#         if has_header:
#             _ = next(reader)  # Read the header in order to skip it
#
#         for line in reader:
#             result = process_func(line)
#             if result_func is not None and result is not None:
#                 result_func(result)




def main():
    """ """


if __name__ == '__main__':
    main()
