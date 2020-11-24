import psycopg2
from psycopg2 import errors
from model import * 
from view import *


def database(request):
	command_view()

	if request=='random':
		db_random()
	elif request=='search':
		db_search()
	elif request=='delete':
		db_delete()
	elif request=='insert':
		db_add()
	elif request=='update':
		db_update()
	
database()