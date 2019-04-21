import os
import sys
import csv
import sqlparse
from pprint import pprint
from collections import OrderedDict

sql = 'select A, D from table1,table2 where A = 922 AND D = 14421;'

tables_col = OrderedDict()
tables_row = OrderedDict()

def check_tables(tables):
	for table in tables:
		if not os.path.exists(table + '.csv'):
			print_message('Table does not exist.', False)

def check_ambiguous(tables, columns):
	for col in columns:
		if '.' in col:
			if col not in tables_col[col[:col.find('.')]].keys():
				print_message(col[:col.find('.')] + ' does not contain column ' + col[col.find('.')+1:], False)
		else:
			count = 0
			for table in tables:
				if str(table + '.' + col) in tables_col[table].keys():
					count += 1
			if count == 0:
				print_message('No such column ' + col, False)
			if count > 1:
				print_message(col + ' is ambiguous', False)

def get_header(tables, columns):
	header = []
	for col in columns:
		if '.' in col:
			header.append(col)
		else:
			for table in tables:
				if str(table + '.' + col) in tables_col[table].keys():
					header.append(table + '.' + col)
	return header

def get_table_header(tables):
	table_header = []
	for table in tables:
		table_header += tables_col[table].keys()
	return table_header

def print_message(msg, type):
	if type:
		print '[SUCCESS] ' + msg
	else:
		print '[ERROR] ' + msg
	sys.exit()

def print_all(table):
	line = ''
	header = tables_col[table].keys()
	for key in header:
		line += key + ','
	line = line[:-1]
	print line
	length = len(tables_col[table][header[0]])

	for i in range(length):
		line = ''
		for h in header:
			line += str(tables_col[table][h][i]) + ','
		line = line[:-1]
		print line

def print_table(tables, columns):
	tname = ''
	line = ''
	for table in tables:
		tname += table

	for col in columns:
		line += col + ','
	print line[:-1]

	length = len(tables_col[tname][columns[0]])

	for i in range(length):
		line = ''
		for col in columns:
			line += str(tables_col[tname][col][i]) + ','
		line = line [:-1]
		print line

def print_aggregrate(table, tokens):
	stmt = tokens.split(',')
	stmt = [ s.strip() for s in stmt ]
	functions = ['max', 'min', 'avg', 'sum']
	cols = []
	ops = []
	for s in stmt:
		found = False
		if '(' in s:
			fun = s[:s.find('(')]
			if fun in functions:
				found = True
				ops.append(fun)
				cols.append(table + '.' + s[s.find('(')+1:s.find(')')])
				break
		if not found:
			print_message('Incorrect Syntax', False)

	line = ''

	for col in cols:
		if col not in tables_col[table].keys():
			print_message('No such column as ' + col + ' in ' + table, False)

	for i in range(len(ops)):
		if ops[i] == 'max':
			line += str(max(tables_col[table][cols[i]])) + ','
		if ops[i] == 'min':
			line += str(min(tables_col[table][cols[i]])) + ','
		if ops[i] == 'sum':
			line += str(sum(tables_col[table][cols[i]])) + ','
		if ops[i] == 'avg':
			line += str(float(sum(tables_col[table][cols[i]]))/len(tables_col[table][cols[i]])) + ','
	print line[:-1]

def print_where():
	pass

def is_aggregate(tokens):
	aggregate = ['max', 'min', 'sum', 'avg']

	for agg in aggregate:
		if agg in tokens:
			return True
	return False

def is_all(tokens):
	stmt = str(str(tokens).split(','))
	print stmt
	if '*' in stmt:
		stmt = [ s.strip() for s in stmt ]
		print stmt
		if len(stmt) > 1:
			print_message('Invalid Query', False)
		if stmt[0] == '*':
			return True
	return False

def is_where(tokens):
	if 'where' in str(tokens):
		return True
	return False

def to_col(data, table_header, tables):
	global tables_col

	pos = 0

	tname = ''

	for table in tables:
		tname += table

	if tname in tables_col.keys():
		return

	tables_col[tname] = OrderedDict()

	for c in range(len(data[0])):
		column = []
		for r in range(len(data)):
			column.append(int(data[r][c]))
		tables_col[tname][table_header[pos]] = column
		pos += 1

def cross_product(tables):

	table1 = tables_row[tables[0]]

	for t in tables[1:]:
		table = []

		table2 = tables_row[t]

		for i in range(len(table1)):
			for j in range(len(table2)):
				table.append(table1[i]+table2[j])

		table1 = table

	return table1

def process_table(table):
	global tables_row, tables_col
	rows = []
	content = []
	with open(table + '.csv', "r") as csv_file:
	    csv_reader = csv.reader(csv_file, delimiter=',')
	    for lines in csv_reader:
	      rows.append(lines)

	tables_row[table] = rows

	for c in range(len(rows[0])):
		column = []
		for r in range(len(rows)):
			column.append(int(rows[r][c]))
		content.append(column)
	return content

def process_metadata():
	global tables_col
	filename = 'metadata.txt'
	content = []
	with open(filename, 'r') as file:
		content = file.readlines()

	table = ''

	data = []
	pos = 0

	for line in content:
		if line.endswith('\r\n'):
			line = line[:-2]
		if line == '<end_table>':
			continue
		if line == '<begin_table>':
			pos = 0
			flag = True
			continue
		if flag:
			table = line
			tables_col[table] = OrderedDict()
			data = process_table(table)
			flag = False
			continue
		tables_col[table][table + '.' + line] = data[pos]
		pos += 1

def process_query(query):
	parsed = sqlparse.parse(query)
	stmt = parsed[0]
	tokens = stmt.tokens

	if len(tokens) < 7 or str(tokens[len(tokens)-1])[-1] != ';':
		print_message('Invalid SQL query', False)

	print tokens

	if stmt.get_type() == 'SELECT':

		cols = map(str.strip, str(tokens[2]).split(','))

		tables = map(str.strip, str(tokens[6]).split(','))

		if is_aggregate(str(tokens[2])):
			if len(tables) == 1:
				print_aggregrate(tables[0], str(tokens[2]))
				print_message('Query executed successfully', True)
			else:
				print_message('Only single table supported', False)

		print str(tokens[2])

		if is_all(tokens[2]):
			if len(tables) == 1:
				print_all(tables[0])
				print_message('Query executed successfully', True)
			else:
				print_message('Only single table supported', False)	

		check_tables(tables)

		check_ambiguous(tables, cols)

		header = get_header(tables, cols)

		table_header = get_table_header(tables)

		data = []

		if len(tables) == 1:
			data = tables_row
		else:
			data = cross_product(tables)
			to_col(data, table_header, tables)

		if len(tokens) > 8:
			if is_where(str(tokens[8])):
				tokens = map(str, tokens)
				print tokens
				sys.exit()

		print_table(tables, header)

	else:
		print_message('Only select query supported now', False)	

if __name__ == '__main__':
	s = ''

	for a in sys.argv[1:]:
		if a == '\*':
			a = '*'
		s += str(a) + ' '

	process_metadata()

	process_query(s[:-1])

