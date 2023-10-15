import glob
import os
import csv
import shutil
import sh


def split_csv(filename: str, max_lines: int, output_dir='.', with_header=True):
    """Split a large csv file to multiple files where each file contains at most `max_lines` lines

    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File '{filename}' not found.")

    if max_lines <= 0:
        raise ValueError("max_lines must be greater than 0.")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Open the input CSV file for reading
    with open(filename, 'r', newline='') as input_file:
        reader = csv.reader(input_file)

        # Initialize variables for the output files
        current_output_file = open(f"{output_dir}/output_1.csv", 'w')
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
                current_output_file = open(f"{output_dir}/output_{len(output_files) + 1}.csv", 'w', newline='')
                writer = csv.writer(current_output_file)
                if with_header:
                    writer.writerow(header)
                output_files.append(current_output_file)

            writer.writerow(row)
            current_line_count += 1

        # Close all output files
        for file in output_files:
            file.close()

    print(f"Split {filename} into {len(output_files)} smaller CSV files in the '{output_dir}' directory.")


def process_csv(filename: str, process_func, result_func, has_header=True):
    """The process_csv function takes a csv file, a process_func function and result_func function

    It reads the csv lines one by one and passes them to the process_func

    If the process_func returns result that is not None it passes it to the result_func

    If there is a header it skips it
    """
    with open(filename, 'r', newline='') as csv_file:
        reader = csv.reader(csv_file)
        if has_header:
            _ = next(reader)  # Read the header in order to skip it

        for line in reader:
            result = process_func(line)
            if result is not None:
                result_func(result)


def merge_sqlite_dbs(base_name, table, output_db):
    """
    Merge data from multiple SQLite database files into a single database.

    This function combines data from multiple SQLite database files with names starting
    with the specified `base_name` and having the same table structure. The data from
    these files is merged into the `output_db`, preserving the table schema.

    :param base_name: The base name for the input database files. Files with names
                     starting with this base name will be considered for merging.
    :param table: The name of the table in the SQLite databases to be merged.
    :param output_db: The name of the output SQLite database file that will contain
                     the merged data.

    :raises RuntimeError: If no matching database files are found with the provided base name.

    This function operates as follows:
    1. Identifies all database files in the current directory with names starting
       with `base_name`.
    2. Copies the the database file as is  to the `output_db`, creating the output
       database with the same schema.
    3. Exports data from the other input database files using the SQLite3 CLI and
       imports it into the `output_db`, preserving the table structure.

    Example usage:
    merge_sqlite_dbs("db_", "my_table", "merged_db.db")

    Note:
    - Only a single table is copied from all the input databases
    - The input files should have the same table structure.
    - If the table has a primary key field it MUST NOT have duplicate values in the input databases
    - Be cautious when merging data, as no schema compatibility checks are performed.
    """
    input_files = glob.glob(f'{base_name}*.db')
    if not input_files:
        raise RuntimeError('No matching DB files')

    # copy the first file to the output file (so, the output file will have the schema already
    shutil.copyfile(input_files[0], output_db)

    # dump data from other input files and import into the output file
    for f in input_files[1:]:
        sh.sqlite3(f, f'.mode insert {table}', '.out db.sql', f'select * from {table}')
        sh.sqlite3(output_db, '.read db.sql')


if __name__ == '__main__':
    merge_sqlite_dbs('db', 'x', 'output.db')
