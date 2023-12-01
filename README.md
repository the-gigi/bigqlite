# Bigqlite

Bigqlite processes large csv files, by splitting them into multiple smaller csv files,
processing in parallel each csv file (by running a custom `process()` function) and saves the
results to a sqlite DB per csv file. Then, it merges all the small sqlite DBs into a single sqlite DB 

It is designed to be used as a library in your application. The Engine class provides
a `run()` method, which accepts several parameters:

```
    def run(self,
            csv_filename: str,
            max_lines: int,
            template_db_filename: str,
            process_func: callable,
            output_dir=".",
            with_header=True) -> str:
```

- `csv_filename` is a path to a large csv file.
- `max_lines` is the number of lines in each small csv_file (determines how to split the big file)
- `template_db_filename` is an empty sqlite DB with the correct schema to insert the data from the csv file
- `process_func`: a function that is run on each row from the csv file. It can modify the row or skip it by returning None
- `output_dir`: the directory to store all the intermediary files as well as the final sqlite DB - output.db
- `with_header`: a boolean that tells the program if the csv file has a header or just data

# Requirements

This is a Python 3 program.

Make sure you have [Python 3](https://www.python.org/downloads/) installed.

# Installation

## MacOS

Install pyenv and poetry:
- [pyenv](https://github.com/pyenv/pyenv#getting-pyenv)
- [poetry](https://python-poetry.org/docs/#installation)

To set up a virtual environment for the project type this in a terminal window:
```
. ./init.sh
```

## Windows

### Install pyenv-win

In a PowerShell window type:

```
$script_uri="https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1"
Invoke-WebRequest -UseBasicParsing `
                  -Uri  $script_uri `
                  -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```

If you run into problems check out:
https://github.com/pyenv-win/pyenv-win#installation

### Install Poetry

In a PowerShell window type:
```
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

```

If you run into problems check out:
https://python-poetry.org/docs/#installation


### Add poetry to the Windows path

Add the following directory to the PATH environment variable `%APPDATA%\Python\Scripts`

### Set up virtual environment

To set up a virtual environment for the project type this in a PowerShell window:
```
./init.ps1
```

If you have trouble on Windows check out this article:
https://endjin.com/blog/2023/03/how-to-setup-python-pyenv-poetry-on-windows

# Usage

Check out the TestRun class in [engine_test.py](engine_test.py) for an example how to use bigqlite. 
