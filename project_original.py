import pandas as pd
import numpy as np
from db_conn import *
import sys

def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    # 엑셀 파일의 두 시트를 읽기
    df1 = pd.read_excel(excel_file, sheet_name=0, skiprows=4)
    df2 = pd.read_excel(excel_file, sheet_name=1, skiprows=0)

    print(df1.head())
    print(df2.head())

    # 테이블 이름 정의
    movie_table = "university.movie"
    director_table = "university.director"
    genre_table = "university.genre"
    movie_director_table = "university.movie_director"
    country_table = "university.country"

    # 테이블 생성 SQL
    create_sql = f"""
        DROP TABLE IF EXISTS {movie_table};
        DROP TABLE IF EXISTS {director_table};
        DROP TABLE IF EXISTS {genre_table};
        DROP TABLE IF EXISTS {movie_director_table};
        DROP TABLE IF EXISTS {country_table};

        CREATE TABLE {movie_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500),
            eng_title VARCHAR(500),
            year INT,
            m_type VARCHAR(10),
            status VARCHAR(30),
            company VARCHAR(30),
            enter_date DATETIME DEFAULT NOW()
        );

        CREATE TABLE {director_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255)
        );

        CREATE TABLE {genre_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            movie_id INT,
            FOREIGN KEY (movie_id) REFERENCES {movie_table}(id)
        );

        CREATE TABLE {movie_director_table} (
            movie_id INT,
            director_id INT,
            FOREIGN KEY (movie_id) REFERENCES {movie_table}(id),
            FOREIGN KEY (director_id) REFERENCES {director_table}(id)
        );
        
        CREATE TABLE {country_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            movie_id INT,
            FOREIGN KEY (movie_id) REFERENCES {movie_table}(id)
        );
    """
    cur.execute(create_sql)
    conn.commit()

    # 데이터 삽입 SQL
    insert_movie_sql = f"""INSERT INTO {movie_table} (title, eng_title, year, m_type, status, company)
                        VALUES (%s, %s, %s, %s, %s, %s);"""

    insert_director_sql = f"""INSERT INTO {director_table} (name)
                            VALUES (%s);"""

    insert_genre_sql = f"""INSERT INTO {genre_table} (name, movie_id)
                        VALUES (%s, %s);"""

    insert_movie_director_sql = f"""INSERT INTO {movie_director_table} (movie_id, director_id)
                                VALUES (%s, %s);"""
                                
    insert_country_sql = f"""INSERT INTO {country_table} (name, movie_id)
                        VALUES (%s, %s);"""

    # 행을 처리하는 함수
    def process_row(r):
        movie_row = (r[0], r[1], r[2], r[4], r[6], r[8])
        try:
            cur.execute(insert_movie_sql, movie_row)
            movie_id = cur.lastrowid

            director_name = r[7]
            if pd.notna(director_name):
                cur.execute(insert_director_sql, (director_name,))
                director_id = cur.lastrowid
                cur.execute(insert_movie_director_sql, (movie_id, director_id))

            genre_list = str(r[5]).split(',')
            for genre in genre_list:
                cur.execute(insert_genre_sql, (genre.strip(), movie_id))
                
            country_list = str(r[3]).split(',')
            for country in country_list:
                cur.execute(insert_country_sql, (country.strip(), movie_id))

            if (i + 1) % 1000 == 0:
                print(f"{i + 1} rows processed")
        
        except Exception as e:
            print(e)
            print(movie_row)

    # 첫 번째 데이터프레임 처리
    for i, row in df1.iterrows():
        process_row(row)

    # 두 번째 데이터프레임 처리
    for i, row in df2.iterrows():
        process_row(row)
        
    conn.commit()
    
    # 인덱스 생성 SQL
    create_index_sql = f"""
    CREATE INDEX idx_movie_title ON {movie_table}(title);
    CREATE INDEX idx_eng_title ON {movie_table}(eng_title);
    CREATE INDEX idx_movie_year ON {movie_table}(year);
    CREATE INDEX idx_m_type ON {movie_table}(m_type);
    CREATE INDEX idx_status ON {movie_table}(status);
    CREATE INDEX idx_company ON {movie_table}(company);
    CREATE INDEX idx_director_name ON {director_table}(name);
    CREATE INDEX idx_genre_name ON {genre_table}(name);
    CREATE INDEX idx_country_name ON {country_table}(name);
    """
    cur.execute(create_index_sql)
    conn.commit()
    close_db(conn, cur)
    
if __name__ == '__main__':
    read_excel_into_mysql()
