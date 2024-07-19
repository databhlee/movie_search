from flask import Flask, render_template, request
import pymysql
app = Flask(__name__)


# 프론트엔드로부터 데이터 받아오기
@app.route('/', methods=['GET', 'POST'])
def index():
    sqldata = None
    isFirst = True
    attr = ['영화명', '영화명영문', '제작연도', '제작국가',
        '유형', '장르', '제작상태', '감독', '제작사']
    data = [None] * len(attr)
    list = []
    if request.method == 'POST':
        try:
            data[0] = request.form.get('movie_name') #영화명
            data[1] = request.form.get('movie_name_eng') #영화명영문
            data[2] = request.form.get('production_year') #제작연도
            data[3] = request.form.get('country_name') #제작국가
            data[4] = request.form.get('movie_type') #유형
            data[5] = request.form.get('genre') #장르
            data[6] = request.form.get('production_status') #제작상태
            data[7] = request.form.get('director_name') #감독
            data[8] = request.form.get('studio_name') #제작사


            # 받은 데이터를 사용하여 원하는 작업 수행
            # SQL 쿼리 실행
            conn = pymysql.connect(host='localhost', user='root', password='qwer9217', db='university')

            query = """
                SELECT *
                FROM (
                    SELECT 
                    m.title AS 영화명, 
                    m.eng_title AS 영화명영문, 
                    m.year AS 제작연도,
                    GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') AS 제작국가,
                    m.m_type AS 유형,
                    GROUP_CONCAT(DISTINCT g.name ORDER BY g.name SEPARATOR ', ') AS 장르,
                    m.status AS 제작상태,
                    GROUP_CONCAT(DISTINCT d.name ORDER BY d.name SEPARATOR ', ') AS 감독,
                    m.company AS 제작사
                    FROM 
                    movie m
                    LEFT JOIN movie_director mv ON m.id = mv.movie_id
                    LEFT JOIN director d ON mv.director_id = d.id
                    LEFT JOIN genre g ON m.id = g.movie_id
                    LEFT JOIN country c ON m.id = c.movie_id
                    GROUP BY 
                    m.title, m.eng_title, m.year, m.m_type, m.status, m.company
                ) AS joined_table
                WHERE 
            """


            for i in range(len(data)):
                        if data[i] != '':
                            temp = f"{attr[i]} LIKE '%{data[i]}%'"
                            if isFirst:
                                isFirst = False
                            else:
                                temp = ' AND ' + temp
                            query += temp
            curs = conn.cursor(pymysql.cursors.DictCursor)
            curs.execute(query)
            sqldata = curs.fetchall()
            list = []
            for row in sqldata:
                list.append(row)
            curs.close()
            conn.close()
        except Exception as e:
            return str(e)

    return render_template('index.html', list=list)

if __name__ == '__main__':
    app.run(debug=True)
