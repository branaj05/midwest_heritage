echo Creating new virtual environment...
python -m venv venv

echo Activating virtual environment...
call mwh\Scripts\activate

echo Upgrading pip, setuptools, and wheel...
python -m pip install --upgrade pip setuptools wheel

echo Installing packages from requirements.txt...
pip install -r requirements.txt

echo Done! Virtual environment is fresh and ready.
pause