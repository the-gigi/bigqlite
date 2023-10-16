import shutil
import unittest
import csv
import os
import sqlite3
from engine import Engine


class TestSplitCSV(unittest.TestCase):
    def setUp(self):
        self.engine = Engine()

        # Create a temporary directory for testing
        self.test_dir = "test_dir"
        self.tearDown()
        os.makedirs(self.test_dir, exist_ok=True)

        self.test_file = f"{self.test_dir}/test_split_csv.csv"
        # Create a test CSV file with 6 lines and a col_
        with open(self.test_file, 'w', newline='') as test_file:
            writer = csv.writer(test_file)
            writer.writerow(["col_1", "col_2"])  # col_
            for i in range(1, 7):
                writer.writerow([f"value{i}_1", f"value{i}_2"])

    def tearDown(self):
        # Clean up the temporary directory (if exists)
        if not os.path.isdir(self.test_dir):
            return

        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_dir)

    def test_split_csv(self):
        # Test splitting the CSV into 3 files
        self.engine._split_csv(self.test_file, max_lines=2, output_dir=self.test_dir, with_header=True)

        # Check if 3 smaller files were created
        files = os.listdir(self.test_dir)
        self.assertEqual(len(files), 4)

        # Check the contents of the smaller files
        for i in range(1, 4):
            smaller_file = os.path.join(self.test_dir, f"output_{i}.csv")
            with open(smaller_file, 'r', newline='') as f:
                reader = csv.reader(f)
                lines = list(reader)
                self.assertEqual(len(lines), 3)  # col_ + 2 value lines
                self.assertEqual(lines[0], ["col_1", "col_2"])
                self.assertEqual(lines[1], [f"value{i * 2 - 1}_1", f"value{i * 2 - 1}_2"])
                self.assertEqual(lines[2], [f"value{i * 2}_1", f"value{i * 2}_2"])


class TestProcessCSV(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_dir"
        self.tearDown()
        os.makedirs(self.test_dir, exist_ok=True)

        self.test_file = f"{self.test_dir}/test_split_csv.csv"
        test_file = f"{self.test_dir}/test_process_csv.csv"

        self.engine = Engine()
        self.engine.csv_files = [test_file]
        # Create a sample CSV file with a header and data
        with open(test_file, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["col_1", "col_2"])  # Header
            writer.writerow(["a", "1"])
            writer.writerow(["b", "2"])
            writer.writerow(["c", "3"])

            # Create temporary SQLite database files for testing
            db_file = f"{self.test_dir}/test.db"

            # Connect to the test databases and create a table
            conn = sqlite3.connect(db_file)
            conn.execute(f"CREATE TABLE t (col_1 TEXT,\n\tcol_2 INT);")
            conn.commit()

            self.assertTrue(os.path.isfile(db_file))

            self.engine.db_files = [db_file]
            self.engine.db_connections = [conn]
            self.engine.table_name = "t"
            self.engine.process_func = self.process_func

    def tearDown(self):
        if not os.path.isdir(self.test_dir):
            return

        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_dir)

    @staticmethod
    def process_func(line):
        return [line[0], 10 + int(line[1])]

    def test_process_csv_with_header(self):
        self.engine._process_csv(0)
        cursor = self.engine.db_connections[0].cursor()

        # Get a list of all tables in the database
        cursor.execute(f"SELECT * FROM {self.engine.table_name};")
        results = cursor.fetchall()

        expected_results = [
            ("a", 11),
            ("b", 12),
            ("c", 13)
        ]
        self.assertEqual(expected_results, results)


class TestMergeSQLiteDBs(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_dir"
        self.tearDown()
        os.makedirs(self.test_dir, exist_ok=True)

        self.engine = Engine()
        # Create temporary SQLite database files for testing
        self.engine.db_files = [f"{self.test_dir}/output_{i}.db" for i in (1, 2)]
        self.engine.output_dir = self.test_dir
        self.engine.table_name = "t"

        # Connect to the test databases and create a table
        self.engine.db_connections = [sqlite3.connect(db_file) for db_file in self.engine.db_files]
        create_table_cmd = f"CREATE TABLE {self.engine.table_name} (name TEXT);"
        for conn in self.engine.db_connections:
            conn.execute(create_table_cmd)

        # Create the output DB as a copy of one of the DB file with the schemam but without data
        shutil.copyfile(self.engine.db_files[0], f"{self.test_dir}/output.db")

        # Insert sample data
        self.engine.db_connections[0].executemany(f"INSERT INTO t (name) VALUES (?)", [("Alice",), ("Bob",)])
        self.engine.db_connections[1].executemany(f"INSERT INTO t (name) VALUES (?)", [("Charlie",), ("David",)])

        for conn in self.engine.db_connections:
            conn.commit()

    def tearDown(self):
        if not os.path.isdir(self.test_dir):
            return

        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_dir)

    def test_merge_sqlite_dbs(self):
        # Merge the test databases
        self.engine._merge_sqlite_dbs()

        # Check if the output database now contains the combined data
        output_db = f'{self.engine.output_dir}/output.db'
        self.assertTrue(os.path.isfile(output_db))

        expected_data = set((("Alice",), ("Bob",), ("Charlie",), ("David",)))
        with sqlite3.connect(output_db) as conn_output:
            cursor = conn_output.cursor()
            cursor.execute(f"SELECT * FROM {self.engine.table_name};")
            data = set(cursor.fetchall())
            self.assertEqual(data, expected_data)


if __name__ == '__main__':
    unittest.main()
