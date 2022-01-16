#Algorithm of python with MS SQL Server v.1.6

import pyodbc
import copy

#Тестовые данные, используемые на моём пк
#server = 'LAPTOP-DCD8MES6\SQLEXPRESS'
#database = 'Emulation'
#Позже будет переопределено, чтобы алгоритм получал не только название сервера, но также логин и пароль.


user_defined_types = {}

#Подключение к серверу и определённой базе данных
def connect_db(server, database):
	cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
						  'Server='+server+';'
						  'Database='+database+';'
						  'Trusted_Connection=yes;'
						  'autocommit=True')

	cursor = cnxn.cursor()
	get_user_types_data(cursor)
	cursor.close()

	return cnxn


#Получение списка хранимых процедур
def get_procedure_list(cursor, database):
    #Создаём новый запрос, позволяющий получить список кортежей, содержащих схему и название ХП, и вызываем его
	query = f"""
		SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME
		FROM {database}.INFORMATION_SCHEMA.ROUTINES
		WHERE ROUTINE_TYPE = 'PROCEDURE' 
		AND LEFT(ROUTINE_NAME, 3) NOT IN ('sp_', 'xp_', 'ms_')
		AND SPECIFIC_SCHEMA != 'tst' """
	cursor.execute(query)

	#Преобразуем список кортежей в список строк в формате [schema].[name]
	rows = cursor.fetchall()
	rows = [f"{row[0]}.{row[1]}" for row in rows]
	rows.sort()

	return rows


#получение словаря, где ключ - название ХП, значения - её параметры
def get_procedure_params(cursor, sp_list):
	result = {}
	#Для каждой процедуры в списке sp_list получаем данные о её параметрах
	#Записываем как кортежи из трёх элементов, где первый элемент - название, 
	#второй - тип данных, третий - является ли он OUT
	for procedure in sp_list:
		cursor.execute(f"""
			SELECT 
				\'Parameter_name\' = name,
				\'Type\' = type_name(user_type_id),
				\'is_output\' = is_output,
				max_length
			FROM sys.parameters WHERE object_id = object_id(\'{procedure}\')""")
		rows = cursor.fetchall()
		result[procedure] = rows

	procedures_params = copy.deepcopy(result)
	return result


#Функция для вызова хранимой процедуры в MS SQL Server
#cursor - курсор, sp_dict - словарь {процедура : параметры}, name - название, args - аргументы
def exec_procedure(cursor, sp_dict, name, args = []):
	params = sp_dict[name]

	cursor.execute(f"""
		SET NOCOUNT ON
		DECLARE @RC int
		{' '.join([f'DECLARE {param[0]} ' +	
			(param[1] if param[1] not in user_defined_types else  f'dbo.{param[1]}') 
			for param in params ])}

		{set_values(params, args)}

		EXEC @RC = {name}
		{set_SP_params(params)}

		SELECT {', '.join([param[0] for param in params if param[2]])}""")

	result = cursor.fetchall()
	return list(result)


def set_SP_params(params):
	result = []

	for i, param in enumerate(params):
		if param[2]:
			result.append(f'{param[0]} = {param[0]} OUTPUT')
		else:
			result.append(f'{param[0]} = {param[0]}')
	return ', '.join(result)


#Получаем словарь, где ключ - пользовательский табличный тип, значение - список имён столбцов этого типа
def get_user_types_data(cursor):
	cursor.execute(f"""
		SELECT
			TYPE.name,
			COL.name
		FROM sys.table_types TYPE
		JOIN sys.columns COL ON COL.object_id = TYPE.type_table_object_id
		ORDER BY TYPE.name""")
	rows = cursor.fetchall()
	for row in rows:
		if row[0] in user_defined_types:
			user_defined_types[row[0]].append(row[1])
		else:
			user_defined_types[row[0]] = [row[1]]


#Установказначений для всех параметров ХП
def set_values(params, args):
	result = []

	for i, param in enumerate(params):
		if param[1] in user_defined_types:
			result.append(set_user_type_values(param, args[i]))
			continue
		if not param[2]:
			result.append(f'SET {param[0]} = {args[i]}')

	return ' '.join(result)


#Установка значений для параметров, принадлежащих к пользовательским табличным типам
def set_user_type_values(param, value_list):
	result = []

	for value in value_list:
		result.append(f'INSERT INTO {param[0]}({", ".join([col for col in user_defined_types[param[1]]])}) ' 
			+ f'VALUES {value}')
	return ' '.join(result)


#Получение столбцов columns из таблицы table
def get_table_data(cursor, table, columns='*'):
	cursor.execute(f'SELECT {columns} FROM {table}')
	result = cursor.fetchall()
	return list(map(list, result))


#Запись данных values в таблицу table, где values - список, хранящий списки со значениями полей строк
def set_table_data(cursor, table, values):
	print(f'table_name: {table}')
	print(f'values: {values}')
	cursor.execute(""" """.join([f'INSERT INTO {table} VALUES {tuple(value)}' for value in values]))


#Получить список имен столбов таблицы table
def get_columns(cursor, table):
	cursor.execute(f"""
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = N'{table}'""")
	
	rows = cursor.fetchall()
	return [row[0] for row in rows]


#Получение последнего значения в столбце column таблицы table
def get_last_column_value(cursor, table, column):
	cursor.execute(f"""
		SELECT {column} FROM {table}""")

	result = cursor.fetchall()
	return -1 if result == [] else result[-1][0]


#Сравнение на соответствие списка названий полей таблицы и списка входных параметров
#Если списки идентичны, то возвращает пустой список
def compare_table_params(cursor, table, params):
	result = []
	columns = get_columns(cursor, table)
	
	for i, param in enumerate(params):
		if i == len(columns):
			result = params[i:].copy()
			break
			
	return result

#sp_gen_list - список процедур генерации, sp_opt_list - список процедур оптимизации 
#sp_dict - словарь {Назв_ХП : параметры}
#Возвращает два списка: [{Назв_ХП_ген : ном_вых_знач}, ...] и [{Назв_ХП_ген : ном_вход_знач}, ...]
def get_opt_params_ways(sp_gen_list, sp_opt_list, sp_dict):
	output_params = []
	input_params = []

	#Получение названий входных параметров процедур оптимизации
	sp_opt_params = [param[0] for param in sp_dict[sp_opt_list[0]] if not param[2]]

	for param in sp_opt_params:
		pp_dict = [True, {}] #[is_out, {procedure : params} dictionary]
		for procedure in sp_gen_list:
			for i, p in enumerate(sp_dict[procedure]):
				if p[0] == param:
					pp_dict[0] = True if p[2] else False
					pp_dict[1][procedure] = i
					break

			if pp_dict[1] != {}: 
				break

		if pp_dict[1] != {}:
			# output_params.append(pp_dict)
			
			if pp_dict[0]:
				output_params.append(pp_dict[1])
				input_params.append(None)
			else:
				output_params.append(None)
				input_params.append(pp_dict[1])
			continue

		output_params.append(None)
		input_params.append(None)

	return output_params, input_params


#======================================================================================#

"""
#Пример выполнения функций	
server = input("Имя сервера: ")
database = input("Имя БД: ")
cnxn = connect_db(server, database)
cursor = cnxn.cursor()

print(f'\nВсе пользовательские типы\n')
get_user_types_data(cursor)
print(user_defined_types)

print('\nСписок всех ХП на сервере {server} в БД {database}\n')
sp_test_list = get_procedure_list(cursor, database)
for obj in sp_test_list:
	print(obj)

print('\nСловарь, содержащий процедуры и их парметры\n')
sp_dict = get_procedure_params(cursor, sp_test_list)
for key in sp_dict.keys():
	print(f'{key} : {sp_dict[key]}')


print('\nGример вызова хранимой процедуры:')
print('Установка значений параметров для emu.RndGenType2Obj3:\n')
arguments1 = [[(1,4.5), (2,4.2), (3,3.6), (4,7.1), (5,2.4), (6,5.0), (7,6.3), (8,3.8), (9,7.5), (10,3.5)], 10, 10, 0.25]
print(set_values(sp_dict['emu.RndGenType2Obj3'], arguments1))

print('\nСписок выходных параметров для emu.RndGenType2Obj3:\n')
print(exec_procedure(cursor, sp_dict, 'emu.RndGenType2Obj3', arguments1))



print('\nУстановка значений параметров для emu.OptType2Alg1:\n')
arguments2 = [50, 70, 1000, 4.7, 1.9]
print(set_values(sp_dict['emu.OptType2Alg1'], arguments2))

print('\nСписок выходных параметров для emu.OptType2Alg1:\n')
print(exec_procedure(cursor, sp_dict, 'emu.OptType2Alg1', arguments2))


cursor.close()
cnxn.close()
"""

#======================================================================================#