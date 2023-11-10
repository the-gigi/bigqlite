import csv
import functools
import glob
#import logging
import multiprocessing
import os
import shutil
import sqlite3
#import sys

import sh


class Engine:
    def __init__(self):
        self.csv_files = []
        self.db_files = []

        self.table_name = ""
        self.has_header = True
        self.output_dir = ""
        self.process_func = None
        # self.log_queue = multiprocessing.Queue(-1)
        #
        # log_format = logging.Formatter('[%(asctime)s] - %(message)s')
        # self.log = logging.getLogger(__name__)
        # handler = logging.StreamHandler(sys.stdout)
        # handler.setFormatter(log_format)


    @staticmethod
    def _find_db_table(db_filename: str) -> str:
        """Connect to the target sqlite DB and ge the name of its only table

        If there is more or less than one table it raises an exception
        """
        # Connect to the SQLite database
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        # Get a list of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Verify there is exactly one table
        if len(tables) != 1:
            raise RuntimeError('DB must contain exactly one table')
        return tables[0][0]

    @staticmethod
    def _split_csv(filename: str, max_lines: int, output_dir='.', with_header=True):
        """Split a large csv file to multiple files where each file contains at most `max_lines` lines

        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File '{filename}' not found.")

        if max_lines <= 0:
            raise ValueError("max_lines must be greater than 0.")

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        csv_files = []
        # Open the input CSV file for reading
        with open(filename, 'r', newline='') as input_file:
            reader = csv.reader(input_file)

            # Initialize variables for the output files
            filename = f"{output_dir}/output-1.csv"
            csv_files = [filename]
            current_output_file = open(filename, 'w')
            output_files = [current_output_file]
            writer = csv.writer(current_output_file)
            current_line_count = 0
            header = ''
            if with_header:
                header = next(reader)
                writer.writerow(header)

            # Read and distribute rows to output files
            for row in reader:
                if current_line_count >= max_lines:
                    current_line_count = 0
                    filename = f"{output_dir}/output-{len(output_files) + 1}.csv"
                    csv_files.append(filename)
                    current_output_file = open(filename, 'w', newline='')
                    writer = csv.writer(current_output_file)
                    if with_header:
                        writer.writerow(header)
                    output_files.append(current_output_file)

                writer.writerow(row)
                current_line_count += 1

            # Close all output files
            for file in output_files:
                file.close()

            return csv_files

    def _process_csv(self, index: int, process_func: callable, has_header: bool, out=None):
        """The process_csv function takes an index, a process_func function and an optional has_header boolean

        It reads the csv lines one by one and passes them to the process_func

        If the process_func returns result that is not None it writes it to a sqlite DB

        If there is a header it skips it
        """
        if out is not None:
            out(f'{index}: start')
        filename = self.csv_files[index]  # Get current csn file
        db = self.get_db_connection(self.output_dir, index)

        with open(filename, 'r', newline='') as csv_file:
            reader = csv.reader(csv_file)
            if has_header:
                _ = next(reader)  # Read the header in order to skip it

            for i, line in enumerate(reader):
                if out is not None:
                    out('{index}: line {i}')

                # run the current line though the process func
                result = process_func(line)

                # If the result is None, skip it
                if result is None:
                    continue

                # Write the result to the corresponding sqlite DB via its cursor
                placeholders = ", ".join("?" * len(result))
                cmd = f"INSERT INTO {self.table_name} VALUES ({placeholders});"
                try:
                    db.execute(cmd, result)
                except Exception as e:
                    out(f'{index}: error - {e.message}')
                    raise e
        db.commit()

    def _merge_sqlite_dbs(self):
        """
        Merge data from multiple SQLite database files into a single database.

        This function combines data from multiple SQLite database files with names starting
        with `output` and having the same table structure. The data from
        these files is merged into one big output.db preserving the table schema.

        :raises RuntimeError: If no matching database files are found

        This function operates as follows:
        1. Identifies all database files in the output directory
        2. Copies the first database file as is to output.db creating the output
           database with the same schema.
        3. Exports data from the other input database files using the SQLite3 CLI and
           imports it into output.db, preserving the table structure.

        Note:
        - Only a single table is copied from all the input databases
        - The input files should have the same table structure.
        - If the table has a primary key field it MUST NOT have duplicate values in the input databases
        - Be cautious when merging data, as no schema compatibility checks are performed.
        """
        input_files = glob.glob(f'{self.output_dir}/output-*.db')
        if not input_files:
            raise RuntimeError('No matching DB files')

        output_db = f'{self.output_dir}/output.db'
        # copy the first file to the output file (so, the output file will have the schema already
        shutil.copyfile(input_files[0], output_db)

        # dump data from other input files and import into the output file
        temp_sql_file = f'{self.output_dir}/db.sql'
        for f in input_files[1:]:
            # Dump data to temp SQL file
            commands = [f'.mode insert {self.table_name}',
                        f'.out {temp_sql_file}',
                        f'select * from {self.table_name}']
            sh.sqlite3(f, commands)

            # Import temp SQL file into output DB
            sh.sqlite3(output_db, f'.read {temp_sql_file}')

    @staticmethod
    def get_db_filename(output_dir, i):
        return f'{output_dir}/output-{i + 1}.db'

    @staticmethod
    def get_db_connection(output_dir, i):
        filename = Engine.get_db_filename(output_dir, i)
        return sqlite3.connect(filename)

    def run(self,
            csv_filename: str,
            max_lines: int,
            template_db_filename: str,
            process_func: callable,
            output_dir=".",
            with_header=True) -> str:
        if not os.path.isfile(csv_filename):
            raise RuntimeError(f'no such file: {csv_filename}')

        if not os.path.isfile(template_db_filename):
            raise RuntimeError(f'no such file: {template_db_filename}')

        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        if process_func is None:
            raise RuntimeError(f'process func cannot be None')

        self.output_dir = output_dir
        self.has_header = with_header
        self.table_name = self._find_db_table(template_db_filename)

        self.csv_files = self._split_csv(csv_filename, max_lines, output_dir, with_header)

        # Create corresponding sqlite DB for each csv file with the same schema
        for i in range(len(self.csv_files)):
            db_filename = self.get_db_filename(self.output_dir, i)
            shutil.copyfile(template_db_filename, db_filename)

        # Define output queue
        out = multiprocessing.Queue()

        # Define a partial function that can be used as argument to the pool.Map() method
        f = functools.partial(self._process_csv, process_func=process_func, has_header=with_header)
        # Create a process pool with the number of CPU cores available
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            # Process the csv files in parallel
            results = pool.map(f, range(len(self.csv_files)))

        pool.join()
        # Done processing, merge all small sqlite DBs
        self._merge_sqlite_dbs()

        # Return path to output DB
        return f'{self.output_dir}/output.db'
