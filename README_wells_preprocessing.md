# Run Lab 6 Data Preprocessing

## 1) Install dependencies
```bash
python3 -m pip install --upgrade pip
python3 -m pip install pandas python-dotenv mysql-connector-python pdfplumber PyPDF2
```

## 2) Create `.env` next to the script
```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_user
MYSQL_PASSWORD=your_pass
MYSQL_DB=wells_db
MYSQL_TABLE=wells
```

## 3) Create DB/table (once)
```bash
mysql -u your_user -p -h localhost < wells_schema.sql
```

## 4) Run on a folder
```bash
cd ~/Downloads
python3 wells_preprocessing.py --pdf-dir "./DSCI560_Lab5" --out-csv wells.csv
```
