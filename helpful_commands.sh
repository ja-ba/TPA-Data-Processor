#Setting up and accessing virtual environment
python3.11 -m venv .venv
source .venv/Scripts/activate

#Installing build dependencies
python -m pip install --upgrade pip
python -m pip install build
python -m pip install pre-commit
pre-commit install

#Reactivate venv
deactivate && source .venv/Scripts/activate

# installing dependencies
python -m pip install --upgrade pip
python -m pip install -e '.[test]'


#Building the package
pyproject-build

#Running tox in parallel:
tox -p

#Running specifix tox env:
tox -e py39

#Running tox where env is recreated to install new dependencies:
tox --recreate

# Run and build
tox | tox -e ruff, typecheck, format, build

# Run pytest:
python310 -m pytest -s --cov


# Symlinkgs in windows for
# Navigate to folder with python.exe
# mklink python3.X.exe python.exe
