# bigqlite
Bigqlite processes large csv files and saves them to a sqlite DB

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

```
poetry run python main.py
```
