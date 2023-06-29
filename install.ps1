

$NAUTILUS = Join-Path (Split-Path (Split-Path $PSScriptRoot)) "nautilus_trader"
$PYTOWER = Join-Path (Split-Path (Split-Path $PSScriptRoot)) "pytower"

try {
    & (Join-Path $NAUTILUS ".venv/Scripts/deactivate") 2>$null
}
catch {
    # Error occurred, but we are suppressing the error message
}

Remove-Item -Path (Join-Path $NAUTILUS ".venv") -Recurse -Force

poetry install --with dev,test --all-extras

& (Join-Path $NAUTILUS ".venv/Scripts/activate.ps1")

pip install -r (Join-Path $PYTOWER "requirements.txt")

pip install (Join-Path $PYTOWER "TA_Lib-0.4.24-cp310-cp310-win_amd64.whl")