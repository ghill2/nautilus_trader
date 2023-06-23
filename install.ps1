$PARENT = Split-Path $MyInvocation.MyCommand.Path
$GPARENT = Split-Path (Split-Path $MyInvocation.MyCommand.Path)

# if (Get-Command -Name "deactivate" -ErrorAction SilentlyContinue) {}

$NAUTILUS = Join-Path $GPARENT "nautilus_trader"
$PYTOWER = Join-Path $GPARENT "pytower"

try {
    & (Join-Path $NAUTILUS ".venv/Scripts/deactivate") 2>$null
}
catch {
    # Error occurred, but we are suppressing the error message
}

Remove-Item -Path (Join-Path $NAUTILUS ".venv") -Recurse -Force

# Set-Location (Join-Path $GPARENT "nautilus_trader");
poetry install --with dev,test --all-extras


# Set-Location (Join-Path $GPARENT "pytower");

# execute the script within the current PowerShell session
# & ~/BU/projects/nautilus_trader/.venv/Scripts/activate.ps1
& (Join-Path $NAUTILUS ".venv/Scripts/activate.ps1")
pip install -r (Join-Path $PYTOWER "requirements.txt")

pip install (Join-Path $PYTOWER "TA_Lib-0.4.24-cp310-cp310-win_amd64.whl")