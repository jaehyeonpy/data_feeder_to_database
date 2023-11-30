import time
import signal

import pymysql

from datetime import datetime
from faker import Faker


# TODO: 
# using ORM, change hard-coded sql statements,
# using other database connector, improve speed,
# using multi-processing, improve speed,
# using command-line interface, specify host, user, password,
class Database:
    def __init__(self):
        self.conn = pymysql.connect(
                host='localhost',
                user='root',
                password='',
                cursorclass=pymysql.cursors.DictCursor,
            )
        self.cur = self.conn.cursor()

        self.cur.execute('DROP DATABASE IF EXISTS shopping_mall')
        self.cur.execute('CREATE DATABASE shopping_mall')
        self.cur.execute('USE shopping_mall')
        self.cur.execute('DROP TABLE IF EXISTS orders')
        self.cur.execute('''
            CREATE TABLE orders(
                id BIGINT AUTO_INCREMENT PRIMARY KEY, 
                user_name VARCHAR(18), 
                address TEXT, 
                tel_num VARCHAR(13), 
                total_price BIGINT, 
                created_at DATETIME
            ) 
            ''')
     
    def insert(self, user_name, address, tel_num, total_price, created_at):
        # mysql truncates fraction point of created_at.
        self.cur.execute('''
            INSERT INTO orders (
                user_name, 
                address, 
                tel_num, 
                total_price, 
                created_at
            ) VALUES (
                "{user_name}", 
                "{address}", 
                "{tel_num}",
                {total_price},
                "{created_at}"
            )
            '''
            .format(
                user_name=user_name,
                address=address,
                tel_num=tel_num, 
                total_price=total_price,
                created_at=created_at
            )) 
        self.conn.commit()
                
    def update(self, id, user_name, address, tel_num, total_price, created_at):
        self.cur.execute('''
            UPDATE orders 
                SET
	                user_name="{user_name}", 
                    address="{address}", 
	                tel_num="{tel_num}", 
                    total_price={total_price}, 
                    created_at="{created_at}"
                WHERE id={id}
            '''
            .format(
                user_name=user_name,
                address=address,
                tel_num=tel_num, 
                total_price=total_price,
                created_at=created_at,
                id=id
            )) 
        self.conn.commit()

    def delete(self, id):
        self.cur.execute('DELETE FROM orders WHERE id={id}'.format(id=id)) 
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

db = Database()
fake_generator = Faker('ko_KR') # TODO: change language by command-line interface, and generate more fake things than korean using member functions.

def exit_closing_connection(signum, frame):
    print('Closed the connection to the database.')
    db.close()
    exit(0)

signal.signal(signal.SIGINT, exit_closing_connection)

id = 0
row_for_update_or_delete = None

TOTAL_PRICE_MIN = 1
TOTAL_PRICE_MAX = 9223372036854775807

# TODO: specify through command-line arguments.
RNDINT_MIN = 1
RNDINT_MAX = 99
UPDATE_BORDER = 90
DELETE_BORDER = 80
DELAY_TIME = 8


print('Please press CTRL+C to kill the process, and close the connection to the database!')

while True:
    id += 1
    user_name = fake_generator.name()
    address = fake_generator.address()
    tel_num = fake_generator.phone_number() # fake.phone_number() includes telephone number and phone number.
    total_price = fake_generator.random_int(min=TOTAL_PRICE_MIN, max=TOTAL_PRICE_MAX)
    created_at = datetime.now()

    db.insert(user_name, address, tel_num, total_price, created_at)
    print('inserted: ', {'id': id, 'user_name': user_name, 'address': address, 'tel_num': tel_num, 'total_price': total_price, 'created_at': created_at})

    rndint = fake_generator.random_int(RNDINT_MIN, RNDINT_MAX)

    if row_for_update_or_delete == None:
        row_for_update_or_delete = id
    else:
        if UPDATE_BORDER <= rndint:
            db.update(row_for_update_or_delete, user_name, address, tel_num, total_price, created_at)             
            print('updated: ', {'id': row_for_update_or_delete, 'user_name': user_name, 'address': address, 'tel_num': tel_num, 'total_price': total_price, 'created_at': created_at})        

        elif DELETE_BORDER <= rndint:
            db.delete(row_for_update_or_delete)
            print('deleted: ', {'id': row_for_update_or_delete})   
            
            row_for_update_or_delete = None

    time.sleep(DELAY_TIME) 