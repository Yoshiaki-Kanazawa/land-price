import sys
import pandas as pd
import mysql.connector

# Constants
IMPORT_CSV = sys.argv[1]
MYSQL_HOST = sys.argv[2]
MYSQL_USER = sys.argv[3]
MYSQL_PASSWORD = sys.argv[4]
MYSQL_DATABASE = "land_price"
CSV_HEADER = ("code", "prefecture_name", "city_name", "prefecture_kana_name", "city_kana_name", "empty1", "empty2")

code_df = pd.read_csv(IMPORT_CSV, header=None, names=CSV_HEADER, skiprows=1)

with mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE) as conn:
    curs = conn.cursor()
    insert_query = """
        INSERT IGNORE INTO national_local_government_code (
            code,
            prefecture_name,
            prefecture_kana_name,
            city_name,
            city_kana_name
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s
        )
    """

    for index, row in code_df.iterrows():
        curs.execute(
            insert_query,
            (
                row.code,
                row.prefecture_name,
                row.prefecture_kana_name,
                row.city_name if not pd.isnull(row.city_name) else "",
                row.city_kana_name if not pd.isnull(row.city_kana_name) else ""
            )
        )

    conn.commit()