import psycopg2
import time
from psycopg2 import errors
import sys

def information_column(table,column):
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()
	cursor.execute(f"SELECT {column} FROM {table}")
	values=cursor.fetchall()
	print(values)
	cursor.close()
	conect.close()

def d_update(table1,table2,column):
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	old=input('Old name > ')
	new=input('New name > ')

	if(table1=='seller'):
		try:
			cursor.execute(f"WITH key AS(UPDATE {table1} SET {column} = '{new}' WHERE {column} = '{old}' )UPDATE shopper SET {column}='{new}' WHERE {column} = '{old}'")
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')
	elif(table1=='shopper'):
		try:
			cursor.execute(f"WITH key AS(UPDATE {table1} SET {column} = '{new}' WHERE {column} = '{old}' )UPDATE seller SET {column}='{new}' WHERE {column} = '{old}'")
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')
	elif(table1=='price'):
		try:
			cursor.execute(f"WITH key AS(UPDATE {table1} SET {column} = '{new}' WHERE {column} = '{old}' )UPDATE article SET {column}='{new}' WHERE {column} = '{old}'")
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')
	elif(table1=='article'):
		try:
			cursor.execute(f"WITH key AS(UPDATE {table1} SET {column} = '{new}' WHERE {column} = '{old}' )UPDATE price SET {column}='{new}' WHERE {column} = '{old}'"
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')

	cursor.close()
	conect.close()

def update(name,column):
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	old=input('Old name > ')
	new=input('New name > ')
	try:
		cursor.execute(f'UPDATE {name} SET {column} = {new} WHERE {column} = {old}')
	except psycopg2.Error as warning:
			print(warning.pg.code)
			print(f'WARNING: {warning}')

	cursor.close()
	conect.close()

def db_update():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table=input("Input table name > ")
	column=input('Column name > ')
	information_column(table,column)
	if (table=='shopper' or table=='seller') and column=='seller_data':
		try:
			d_update('shopper','seller','seller_data')
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')
	elif (table=='article' or table=='price') and column=='price':
		try:
			d_update('article','price','price')
		except psycopg2.Error as warning:
			print(warning.pgcode)
			print(f'WARNING: {warning}')
	else:
		try:
			update(table,column)
		except psycopg2.Error as warning:
			print(warning.pg.code)
			print(f'WARNING: {warning}')

	cursor.close()
	conect.close()

def db_add():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table=input("Input table name > ")
	i=0
	keys=[]
	global code_null  
	code_null = '[null]'
	if table=='price':
		print("Enter 2 value > ")
		while(i<2):
			key=input()
			keys.append(key)
			i+=1
		cursor.execute(f"WITH table AS(INSERT INTO {table} VALUES({keys[0]},{keys[1]}))INSERT INTO article VALUES ({code_null},{keys[0]},{code_null})")
	if table=='article':
		print('Enter 3 value > ')
		while(i<3):
			key=input()
			keys.append(key)
			i+=1
		cursor.execute(f"WITH table AS(INSERT INTO {table} VALUES({keys[0]},{keys[1]},{keys[2]}))INSERT INTO price VALUES ({keys[1]},{code_null})")
	if table=='shopper':
		print('Enter 3 value > ')
		while(i<3):
			key=input()
			keys.append(key)
			i+=1
		cursor.execute(f"WITH table AS(INSERT INTO {table} VALUES({keys[0]},{keys[1]},{keys[2]}))INSERT INTO seller VALUES ({keys[2]},{code_null},{code_null})")
	if table=='seller':
		print('Enter 3 value > ')
		while(i<3):
			key=input()
			keys.append(key)
			i+=1
		cursor.execute(f"WITH table AS(INSERT INTO {table} VALUES({keys[0]},{keys[1]},{keys[2]}))INSERT INTO shopper VALUES ({code_null},{code_null},{keys[1]})")
	
	cursor.close()
	conect.close()


def db_delete():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table=input("Input table name > ")
	column=input('Enter column > ')
	information_column(table,column)
	value=input('Enter value > ')
	if table=='price':
		information_column('price','price')
		db_key=input('Enter price value > ')
		cursor.execute(f'WITH pst AS(DELETE FROM price WHERE {column} = {value})DELETE FROM article WHERE price = {db_key}')
	elif table=='article':
		information_column('article','price')
		db_key=input('Enter price value > ')
		cursor.execute(f'WITH pst AS(DELETE FROM article WHERE {column} = {value})DELETE FROM price WHERE price = {db_key}')
	elif table=='shopper':
		information_column('shopper','seller_data')
		db_key=input('Enter price value > ')
		cursor.execute(f'WITH pst AS(DELETE FROM shopper WHERE {column} = {value})DELETE FROM seller WHERE seller_data = {db_key}')
	elif table=='seller':
		information_column('price','price')
		db_key=input('Enter price value > ')
		cursor.execute(f'WITH pst AS(DELETE FROM seller WHERE {column} = {value})DELETE FROM shopper WHERE seller_data = {db_key}')
	else:
		print("wrong table name")

	cursor.close()
	conect.close()

def db_random():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	table=input("Input table name > ")
	random_size=input('Enter random size > ')
	if table =='article':
		cursor.execute(f'WITH rndm AS(INSERT INTO article SELECT chr(trunc(65+random()*35000)::int),(trunc(65+random()*35000)::int),chr(trunc(65+random()*35000)::int) FROM generate_series(1,{random_size}) RETURNING price)INSERT INTO price SELECT price FROM rndm')
	elif table =='price':
		cursor.execute(f'WITH rndm AS(INSERT INTO price SELECT (trunc(65+random()*35000)::int),(trunc(65+random()*35000)::int) FROM generate_series(1,{random_size}) RETURNING price)INSERT INTO article SELECT price FROM rndm')
	elif table =='shopper':
		cursor.execute(f'WITH rndm AS(INSERT INTO shopper SELECT chr(trunc(65+random()*35000)::int),chr(trunc(65+random()*35000)::int),chr(trunc(65+random()*35000)::int) FROM generate_series(1,{random_size}) RETURNING seller_data)INSERT INTO seller SELECT seller_data FROM rndm')
	elif table =='seller':
		cursor.execute(f'WITH rndm AS(INSERT INTO seller SELECT chr(trunc(65+random()*35000)::int),chr(trunc(65+random()*35000)::int),(trunc(65+random()*35000)::int) FROM generate_series(1,{random_size}) RETURNING seller_data)INSERT INTO shopper SELECT seller_data FROM rndm')
	cursor.close()
	conect.close()

def db_search():
	conect=psycopg2.connect(dbname='DimaDB',user='postgres',password='dertyloik',host='localhost',port=5432)
	conect.set_session(autocommit=True)
	cursor=conect.cursor()

	n = int(input("Input quantity of attributes to search by >>> "))
	column=[]
	for h in range(0,n):
		column.append(str(input(f"Input name of the attribute number {h+1} to search by >>> ")))
	print(column)
	tables = []
	types = []
	if n == 2:
		curso_names_str = f"SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE information_schema.columns.column_name LIKE '{column[0]}' INTERSECT ALL SELECT table_name FROM information_schema.columns WHERE information_schema.columns.column_name LIKE '{column[1]}'"
	else:
		curso_names_str = "SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE information_schema.columns.column_name LIKE '{}'".format(column[0])
	print("\ncol_names_str:", curso_names_str)
	cursor.execute(curso_names_str)
	curso_names = (cursor.fetchall())
	for tup in curso_names:
		tables += [tup[0]]
	if 'student_teacher' in tables:
		tables.remove('student_teacher')
		print(tables)
	for s in range(0,len(column)):
		for k in range(0,len(tables)):
			cursor.execute(f"SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{tables[k]}' AND column_name ='{column[s]}'")
			type=(cursor.fetchall())
			for j in type:
				types+=[j[0]]
	print(types)
	if n == 1:
		if len(tables) == 1:
			if types[0] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by >>> ")
				start_time=time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("Time:%s seconds"%(time.time()-start_time))
			elif types[0] == 'integer':
				left_limits = input("Enter left limit")
				right_limits = input("Enter right limit")
				start_time=time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}'")
				print(cursor.fetchall())
				print("Time:%s seconds"%(time.time()-start_time))
		elif len(tables) == 2:
			if types[0] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by >>> ")
				start_time = time.time()
				cursor.execute(f"SELECT {column[0]} FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' UNION ALL SELECT {column[0]} FROM {tables[1]} WHERE {column[0]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))
			elif types[0] == 'integer':
				left_limits = input("Enter left limit")
				right_limits = input("Enter right limit")
				start_time = time.time()
				cursor.execute(f"SELECT {column[0]} FROM {tables[0]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}' UNION ALL SELECT {column[0]} FROM {tables[1]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}' ")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))

	elif n == 2:
		if len(tables) == 1:
			if types[0] == 'character varying' and types[1] == 'character varying':
				i_char = input(f"Input string for {column[0]} to search by >>> ")
				o_char = input(f"Input string for {column[1]} to search by >>> ")
				start_time = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' AND {column[1]} LIKE '{o_char}' ")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))
			elif types[0] == 'character varying' and types[1] == 'integer':
				i_char = input(f"Input string for {column[0]} to search by >>> ")
				left_limits = input(f"Enter left limit for {column[1]}")
				right_limits = input(f"Enter right limit for {column[1]}")
				start_time = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]} LIKE '{i_char}' AND {column[1]}>='{left_limits}' AND {column[1]}<'{right_limits}'")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))
			elif types[0] == 'integer' and types[1] == 'character varying':
				left_limits = input(f"Enter left limit for {column[0]}")
				right_limits = input(f"Enter right limit for {column[0]}")
				i_char = input(f"Input string for {column[1]} to search by >>> ")
				start_time = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{left_limits}' AND {column[0]}<'{right_limits}' AND {column[1]} LIKE '{i_char}'")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))
			elif types[0] == 'integer' and types[1] == 'integer':
				i_left_limits = input(f"Enter left limit for {column[0]}")
				i_right_limits = input(f"Enter right limit for {column[0]}")
				o_left_limits = input(f"Enter left limit for {column[1]}")
				o_right_limits = input(f"Enter right limit for {column[1]}")
				start_time = time.time()
				cursor.execute(f"SELECT * FROM {tables[0]} WHERE {column[0]}>='{i_left_limits}' AND {column[0]}<'{i_right_limits}' AND {column[1]}>='{o_left_limits}' AND {column[1]}<'{o_right_limits}' ")
				print(cursor.fetchall())
				print("Time:%s seconds" % (time.time() - start_time))
	
	cursor.close()
	conect.close()