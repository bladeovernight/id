import sqlite3
import csv
import json

def load_data(file_name):
    items = []

    with open(file_name, "r", encoding='utf-8') as input:
        reader = csv.reader(input, delimiter=";")
        reader.__next__()

        for row in reader:
            if len(row) == 0: continue
            item = dict()
            item['name'] = row[0]
            item['place'] = int(row[1])
            item['prise'] = int(row[2])
            print(item)
            items.append(item)
    return items

def connect(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection




def insert_subitem(db,data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO subitem (id, place, prise) 
        VALUES(
            (SELECT id from tournaments WHERE name = :name),
            :place, :prise)""", data)

    db.commit()

def name_2(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * 
        FROM subitem
        WHERE id = (SELECT id FROM tournaments WHERE name = ?)                
         """, [name])
    items = []
    for row in res.fetchall():
        item = dict(row)
        print(item)
        items.append(item)
    cursor.close()
    return items


def name_stat_2(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(prise) as sum_prise,
            AVG(prise) as avg_prise,
            MIN(prise) as min_prise, 
            MAX(prise) as max_prise
        FROM subitem
        WHERE id = (SELECT id FROM tournaments WHERE name = ?)  

         """, [name])
    items = []
    for row in res.fetchall():
        item = dict(row)
        print(item)
        items.append(item)
    cursor.close()
    return items

def place_2(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            begin,
            (SELECT COUNT(*) FROM subitem WHERE id = id) as place
        FROM tournaments
        ORDER BY place DESC
        LIMIT 38             
         """)
    items = []
    for row in res.fetchall():
        item = dict(row)
        print(item)
        items.append(item)
    cursor.close()
    return items


db = connect('4 work/task_2/2.db')
name_2(db, "Биль 1961")
name_stat_2(db, "Биль 1961")
place_2(db)

with open("name_2.json", 'w',  encoding="utf-8") as f:
    f.write(json.dumps(name_2(db, 'Биль 1961'), ensure_ascii=False))

with open("name_stat_2.json", 'w',  encoding="utf-8") as f:
    f.write(json.dumps(name_stat_2(db, 'Биль 1961'), ensure_ascii=False))

with open("place_2.json", 'w',  encoding="utf-8") as f:
    f.write(json.dumps(place_2(db), ensure_ascii=False))

items = load_data('4 work/task_2/task_2_var_38_subitem.csv')
insert_subitem(db,items)