"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv

employees_data = []
customers_data = []
orders_data = []


def get_employees_data():
    with open("north_data/employees_data.csv", encoding='utf-8') as r_file:
        count = 0
        for line in csv.reader(r_file):
            if count != 0:
                employees_data.append(tuple(line))
            count += 1
    return employees_data


def get_customers_data():
    with open("north_data/customers_data.csv", encoding='utf-8') as r_file:
        count = 0
        for line in csv.reader(r_file):
            if count != 0:
                customers_data.append(tuple(line))
            count += 1
    return customers_data


def get_orders_data():
    with open("north_data/orders_data.csv", encoding='utf-8') as r_file:
        count = 0
        for line in csv.reader(r_file):
            if count != 0:
                orders_data.append(tuple(line))
            count += 1
    return orders_data


conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='2202'
)
try:
    with conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", get_employees_data())
            cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", get_customers_data())
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", get_orders_data())
            cur.execute('SELECT * FROM employees')
            cur.execute('SELECT * FROM customers')
            cur.execute('SELECT * FROM orders')

            rows = cur.fetchall()

            for row in rows:
                print(row)

finally:
    conn.close()
