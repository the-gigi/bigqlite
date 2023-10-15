import unittest
import csv
import os
import sqlite3
from main import (
    split_csv,
    process_csv,
    merge_sqlite_dbs,
)


class TestSplitCSV(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for testing
        self.test_output_dir = "test_split_csv_output"
        self.tearDown()

        self.test_file = "test_split_csv.csv"
        os.makedirs(self.test_output_dir, exist_ok=True)

        # Create a test CSV file with 6 lines and a col_
        with open(self.test_file, 'w', newline='') as test_file:
            writer = csv.writer(test_file)
            writer.writerow(["col_1", "col_2"])  # col_
            for i in range(1, 7):
                writer.writerow([f"value{i}_1", f"value{i}_2"])

    def tearDown(self):
        # Clean up the temporary directory (if exists)
        if not os.path.isdir(self.test_output_dir):
            return

        for root, dirs, files in os.walk(self.test_output_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_output_dir)

    def test_split_csv(self):
        # Test splitting the CSV into 3 files
        split_csv(self.test_file, max_lines=2, output_dir=self.test_output_dir, with_header=True)

        # Check if 3 smaller files were created
        files = os.listdir(self.test_output_dir)
        self.assertEqual(len(files), 3)

        # Check the contents of the smaller files
        for i in range(1, 4):
            smaller_file = os.path.join(self.test_output_dir, f"output_{i}.csv")
            with open(smaller_file, 'r', newline='') as f:
                reader = csv.reader(f)
                lines = list(reader)
                self.assertEqual(len(lines), 3)  # col_ + 2 value lines
                self.assertEqual(lines[0], ["col_1", "col_2"])
                self.assertEqual(lines[1], [f"value{i * 2 - 1}_1", f"value{i * 2 - 1}_2"])
                self.assertEqual(lines[2], [f"value{i * 2}_1", f"value{i * 2}_2"])


class TestProcessCSV(unittest.TestCase):
    def setUp(self):
        # Create a sample CSV file with a header and data
        self.test_file = "test_process_csv.csv"
        with open(self.test_file, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["col_1", "col_2"])  # Header
            writer.writerow(["a", "1"])
            writer.writerow(["b", "2"])
            writer.writerow(["c", "3"])
        self.results = {}

    def tearDown(self):
        # Clean up the sample CSV file
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    @staticmethod
    def process_func(line):
        return [line[0], int(line[1])]

    def result_func(self, row):
        k, v = row
        self.results[k] = v

    def test_process_csv_with_header(self):
        process_csv(self.test_file, self.process_func, self.result_func)
        expected_results = dict(a=1, b=2, c=3)
        self.assertEqual(self.results, expected_results)


class TestMergeSQLiteDBs(unittest.TestCase):
    def setUp(self):
        # Create temporary SQLite database files for testing
        self.db1 = "test_db1.db"
        self.db2 = "test_db2.db"
        self.output_db = "test_output.db"
        self.table = "t"

        self.clear()
        # Connect to the test databases and create a table
        self.conn1 = sqlite3.connect(self.db1)
        self.conn2 = sqlite3.connect(self.db2)
        create_table_cmd = f"CREATE TABLE {self.table} (name TEXT);"
        self.conn1.execute(create_table_cmd)
        self.conn2.execute(f"CREATE TABLE {self.table} (name TEXT);")

        # Insert sample data
        self.conn1.executemany(f"INSERT INTO {self.table} (name) VALUES (?)", [("Alice",), ("Bob",)])
        self.conn2.executemany(f"INSERT INTO {self.table} (name) VALUES (?)", [("Charlie",), ("David",)])

        self.conn1.commit()
        self.conn2.commit()

    def clear(self):
        for f in (self.db1, self.db2, self.output_db, 'db.sql'):
            if os.path.isfile(f):
                os.remove(f)

    def tearDown(self):
        # Clean up the temporary database files
        self.conn1.close()
        self.conn2.close()
        self.clear()

    def test_merge_sqlite_dbs(self):
        # Merge the test databases
        merge_sqlite_dbs("test_db", self.table, self.output_db)

        # Check if the output database now contains the combined data
        conn_output = sqlite3.connect(self.output_db)
        cursor = conn_output.execute(f"SELECT * FROM {self.table};")
        data = set(cursor.fetchall())
        conn_output.close()

        expected_data = set((("Alice",), ("Bob",), ("Charlie",), ("David",)))

        self.assertEqual(data, expected_data)


if __name__ == '__main__':
    unittest.main()
