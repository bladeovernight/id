import pickle
import sqlite3
import json


def get_data(filename):
    with open(filename, mode='br') as file:
        return pickle.load(file)


def connect_to_db(filename):
    connection = sqlite3.connect(filename)
    connection.row_factory = sqlite3.Row
    return connection


def execute_query(db, query, params=[]):
    cursor = db.cursor()
    cursor.execute(query, params)
    data = cursor.fetchall()
    cursor.close()
    return data


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        insert into tournaments(id, name, city, begin, system, tours_count, min_rating, time_on_game)
        values(:id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game)""", data)
    db.commit()
    cursor.close()


def write_data(filename, data):
    with open(filename, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def task1(db, filename, limit=48):
    query = 'select * from tournaments order by tours_count desc limit ?'
    params = [limit]
    ordered_data = execute_query(db, query, params)
    result = [dict(row) for row in ordered_data]

    write_data(filename, result)


def task2(db, filename):
    query = 'select sum(time_on_game) as sum, min(time_on_game) as min, max(time_on_game) as max, avg(time_on_game) as avg from tournaments'
    stats = execute_query(db, query).pop()
    result = dict(stats)

    write_data(filename, result)


def task3(db, filename):
    query = """select city, cast(count() as real) / (select count() from tournaments) as count 
        from tournaments group by city order by count desc"""
    groups = execute_query(db, query)
    result = [dict(row) for row in groups]

    write_data(filename, result)


def task4(db, filename, rating, limit=48):
    query = 'select * from tournaments where min_rating > ? order by begin desc limit ?'
    params = [rating, limit]
    filtered_data = execute_query(db, query, params)
    result = [dict(row) for row in filtered_data]

    write_data(filename, result)


data = get_data('4 work/task_1/task_1_var_38_item.pkl')
connection = connect_to_db('4 work/task_1/1.db')
insert_data(connection, data)

task1(connection, 'ordered_by_tours_count.json')
task2(connection, 'time_on_game_stats.json')
task3(connection, 'grouped_by_city.json')
task4(connection, 'filtered_by_rating.json', 2400)