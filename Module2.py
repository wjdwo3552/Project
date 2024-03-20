from Module1 import connect_to_mysql, execute_query, close_connection
import schedule
import time

def retrieve_and_save_data():
    data = retrieve_data()
    print("데이터 저장됨.")

def retrieve_data():
    conn, cursor = connect_to_mysql(
        host='121.155.34.16',
        port=33063,
        user='sysop',
        password='data001!',
        database='fms'
    )

    query = "SELECT * FROM water_quality_day_tb"
    result = execute_query(cursor, query)

    data_list = []

    for row in result:
        data_list.append(row)

    close_connection(conn, cursor)

    return data_list

if __name__ == "__main__":
    retrieve_and_save_data()
    schedule.every(30).minutes.do(retrieve_and_save_data)

    while True:
        schedule.run_pending()
        time.sleep(1)
