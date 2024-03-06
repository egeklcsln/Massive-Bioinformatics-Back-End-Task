from flask import Flask, request, jsonify
import psycopg2
from config import Config

app = Flask(__name__)

# Veritabanı bağlantı bilgileri
hostName = 'localhost' # Ben Local sunucumda Calistim.
databaseName = '' # veritabanı ismi
username = 'postgres'
password = '' # şifrenizi giriniz
port_id = 5432

# Veritabanına bağlanma fonksiyonu
def connect_to_database():
    conn = psycopg2.connect(
        database=databaseName,
        user=username,
        password=password,
        host=hostName,
        port=port_id
    )
    return conn

# HTTP yanıtlarını oluşturma fonksiyonu
def create_response(data, status_code=200):
    response = {
        "status_code": status_code,
        "data": data
    }
    return jsonify(response), status_code

# "/assignment/query" endpoint'i
@app.route('/assignment/query', methods=['GET', 'POST'])
def query():
    try:
        if request.method == 'GET':
            # Filtre olmadan sayfalama için GET isteğini işleme alma
            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('page_size', 10))
            offset = (page - 1) * page_size

            conn = connect_to_database()
            cur = conn.cursor()

            cur.execute("SELECT * FROM report_output LIMIT %s OFFSET %s", (page_size, offset))
            data = cur.fetchall()

            cur.close()
            conn.close()

            response_data = {
                "page": page,
                "page_size": page_size,
                "count": len(data),
                "results": [dict(zip([desc[0] for desc in cur.description], row)) for row in data]
            }

            return create_response(response_data)

        elif request.method == 'POST':
            # Filtreler, sıralama ve sayfalama ile POST isteğini işleme alma
            request_data = request.json
            filters = request_data.get('filters', {})
            ordering = request_data.get('ordering', [])
            page = int(request_data.get('page', 1))
            page_size = int(request_data.get('page_size', 10))
            offset = (page - 1) * page_size
            query = "SELECT * FROM report_output WHERE TRUE"
            query_params = []

            # Filtreleri uygula
            if filters:
                for column, value in filters.items():
                    if value is None:
                        query += f" AND {column} IS NULL"
                    elif isinstance(value, list):
                        query += f" AND {column} IN ({','.join(['%s' for _ in range(len(value))])})"
                        query_params.extend(value)
                    elif isinstance(value, int) or isinstance(value, float):
                        query += f" AND {column} = %s"
                        query_params.append(value)
                    else:
                        query += f" AND {column} ILIKE %s"
                        query_params.append(f"%{value}%")

            # Sıralamayı uygula
            if ordering:
                order_criteria = []
                for order in ordering:
                    column, direction = list(order.items())[0]
                    order_criteria.append(f"{column} {direction}")

                query += f" ORDER BY {', '.join(order_criteria)}"

            # Sayfalama uygula
            query += " LIMIT %s OFFSET %s"
            query_params.extend([page_size, offset])

            conn = connect_to_database()
            cur = conn.cursor()

            cur.execute(query, query_params)
            data = cur.fetchall()

            cur.close()
            conn.close()

            response_data = {
                "page": page,
                "page_size": page_size,
                "count": len(data),
                "results": [dict(zip([desc[0] for desc in cur.description], row)) for row in data]
            }

            return create_response(response_data)

    except Exception as e:
        error_message = str(e)
        return create_response({"error": error_message}, 500)

if __name__ == '__main__':
    app.run(debug=True)
