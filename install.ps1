$NAUTILUS = Join-Path (Split-Path $PSScriptRoot) "nautilus_trader"
$PYTOWER = Join-Path (Split-Path $PSScriptRoot) "pytower"

try {
    & (Join-Path $NAUTILUS ".venv/Scripts/deactivate") 2>$null
}
catch {
    # Error occurred, but we are suppressing the error message
}

$venv = (Join-Path $NAUTILUS ".venv")
if (Test-Path -Path $venv -PathType Container) {
    Remove-Item -Path $venv -Recurse -Force
}

# poetry env use set the Python used for the Poetry-managed environment, creating it if it doesn't already exist. envs.toml is created to record what the 'current interpreter' for the environment is (as the user has explicitly selected one, instead of letting Poetry select the interpreter implicitly)
poetry env use $(pyenv which python)

# create the virtual environment
# virtualenv -p $(pyenv which python) $venv

# activate the virtual environment
& (Join-Path $NAUTILUS ".venv/Scripts/activate.ps1")

# upgrade pip
# & (Join-Path $NAUTILUS ".venv\Scripts\pip")
pip install --upgrade pip

# install build dependencies # TODO: read from pyproject using toml library
pip install numpy>=1.24.3
pip install Cython==3.0.0b2

# By default, the above command will also install the current project. To install only the dependencies and not including the current project, run the command with the --no-root option like below:
# Set-Location $NAUTILUS  # required before poetry install

poetry install --with dev,test --all-extras --directory=$NAUTILUS --no-root

#  & (Join-Path $NAUTILUS ".venv\Scripts\pip")
pip install -r (Join-Path $PYTOWER "requirements.txt")

# & (Join-Path $NAUTILUS ".venv\Scripts\pip")
pip install (Join-Path $PYTOWER "TA_Lib-0.4.24-cp310-cp310-win_amd64.whl")

# poetry install doesn't build after install because of error
# cannot import name 'BuildBackendException' from 'build' (C:\Users\g1\BU\projects\nautilus_trader\build.py)
Set-Location $NAUTILUS  # required before build
python ./build.py







# parse build requirements in pyproject.toml


# $inproject = poetry config virtualenvs.in-project
# $create = poetry config virtualenvs.in-project
# poetry config virtualenvs.in-project false
# poetry config virtualenvs.create false

# poetry config virtualenvs.in-project $inproject
# poetry config virtualenvs.create $create

