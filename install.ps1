$NAUTILUS = Join-Path (Split-Path $PSScriptRoot) "nautilus_trader"
$PYTOWER = Join-Path (Split-Path $PSScriptRoot) "pytower"
Write-Host $NAUTILUS
Write-Host $PYTOWER

# deactive the active virtual environment (if any)
try {
    & (Join-Path $NAUTILUS ".venv/Scripts/deactivate") 2>$null
}
catch {
    # Error occurred, but we are suppressing the error message
}

# remove the project's environment
$venv = (Join-Path $NAUTILUS ".venv")
if (Test-Path -Path $venv -PathType Container) {
    Remove-Item -Path $venv -Recurse -Force
}

# create the virtual environment
virtualenv -p $(pyenv which python) $venv

# activate the virtual environment
& "$NAUTILUS/.venv/Scripts/activate.ps1"

# make poetry use the new environment
poetry env use "$NAUTILUS/.venv/Scripts/python.exe"

# upgrade pip
python -m pip install --upgrade pip

# install build dependencies # TODO: read from pyproject using toml library
pip install numpy>=1.24.3
pip install Cython==3.0.0b2

# install nautilus dependencies into the active virtual environment
poetry export --with dev,test  --format requirements.txt --without-hashes `
    | ForEach-Object { pip install $_ }

# install pytower dependencies into the active virtual environment
& pip install -r (Join-Path $PYTOWER "requirements.txt")

# install TA-Lib .whl into the active virtual environment
& pip install (Join-Path $PYTOWER "TA_Lib-0.4.24-cp310-cp310-win_amd64.whl")

# build nautilus
Set-Location $NAUTILUS; python ./build.py




# & (Join-Path $NAUTILUS ".venv\Scripts\pip")

# install nautilus packages
# pip install ".[dev,test]"  # does not install the extras

# NOT WORKING! Doesn't install requirements into activated virtualenv?
# Set-Location $NAUTILUS; poetry install --with dev,test --all-extras --directory=$NAUTILUS