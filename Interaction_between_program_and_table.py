# -*- coding: utf-8 -*-
import pyodbc
import pydbwork
import main
import pickle
import os
import pydbwork
import matplotlib.pyplot as plt
import pandas as pd
import string
import numpy as np
import statistics as st
import matplotlib.pyplot as plt

#Выполняем процедуру и возвращаем её выходные данные
def follow_procedure(procedure_name, input_parameters_list, cursor, database):
    sp_test_list = pydbwork.get_procedure_list(cursor, database)
    sp_dict = pydbwork.get_procedure_params(cursor, sp_test_list)
    pydbwork.set_values(sp_dict[procedure_name], input_parameters_list)
    return pydbwork.exec_procedure(cursor, sp_dict, procedure_name, input_parameters_list)

#Преобразуем указания на значения, в значения
def generate_input_processing(procedure_input_data, generation_list, recovered_gen_params):
    for i in range(len(procedure_input_data)):
        if type(procedure_input_data[i]) is dict:
            d_pointer = procedure_input_data[i]
            gen_list_index = generation_list.index(list(d_pointer)[0])
            param_index = d_pointer[list(d_pointer)[0]]
            procedure_input_data[i] = recovered_gen_params[gen_list_index][param_index]
            
#Генерируем метаданные
def generate_meta(input_data, generation_list, cursor, database):
    recovered_gen_params = []
    for i in range(len(generation_list)):
         generate_input_processing(input_data[i], generation_list, recovered_gen_params)
         recovered_gen_params.append(follow_procedure(generation_list[i], input_data[i], cursor, database))
    metadata_tup = recovered_gen_params.pop()
    metadata = []
    for item in metadata_tup:
        metadata.extend(list(item))
    return metadata
         
#Выполняем процедуры оптимизации
def exec_opt_procedures(opt_input, opt_list, cursor, database):
    solution_and_time = []
    for procedure in opt_list:
        solution_and_time.append(list(follow_procedure(procedure, opt_input, cursor, database)[0]))
    return solution_and_time
        
#Генерация решения
def get_exp_solution(opt_list, generation_list, input_data, cursor, database):
    metadata = generate_meta(input_data, generation_list, cursor, database)
    opt_input = get_opt_input(metadata, input_data, generation_list, opt_list, cursor, proc_database)
    print(opt_input)
    solution_list = exec_opt_procedures(opt_input, opt_list, cursor, database)
    for i in solution_list:
        i.reverse()
    solution_list.append(metadata)
    return solution_list, opt_input

#Добавляем столбец в таблицу
def add_column_header(cnxn, table_name, column_name, column_type):
    cnxn.execute('ALTER TABLE '+table_name+' ADD '+column_name+' '+column_type+' NULL')
    cnxn.commit()
    
#Сохраняем результаты эксперемента и доп данные
def save_exp_result(cursor, exp_data, sol_data, sol_tab, exp_tab):
  
    print(exp_data)
    print(sol_data)
    pydbwork.set_table_data(cursor, exp_tab, exp_data)
    pydbwork.set_table_data(cursor, sol_tab, sol_data)

#Генерация строки SQL запроса для сохранения данных логирования
def save_logs(cursor, table, values):
    
    for value in values:
        S=""
        for i in value:
            if type(i) is str and i != "NULL":
                S=f"{S}'{i}', "
            elif type(i) is list:
                S_small = ""
                for j in i:
                    S_small=S_small+str(j)+" "
                S_small=S_small[0:len(S_small)-1]
                S=f"{S}'{S_small}', "
            else:
                S=S+str(i)+", "
        S = S[0:len(S)-2]
        S = S+");"
        S = 'INSERT INTO '+table+' VALUES ('+S
        cursor.execute(S)
        
#Подготовка данных для SQL запроса на сохранение данных логирования
def add_log_inf(cursor, exp_group_id, exp_id, opt_list, gen_list, log_tab):
    log_data = []

    for i in range(0, len(gen_list)):
        log_data.append([exp_group_id, exp_id, i, gen_list[i]])
    for i in range(len(opt_list)):
        log_data.append([exp_group_id, exp_id, i+len(gen_list), opt_list[i]])
    #print(log_data)
    save_logs(cursor, log_tab, log_data)

#Диагностическая информация?
def add_log_data(cursor, exp_group_id, exp_id, opt_list, opt_input, input_data, gen_list, log_data_tab):
    log_data = []
    
    #dimension_and_metadata = dimension
    for i in range(0, len(gen_list)):
        log_data.append([exp_group_id, exp_id, i, gen_list[i], input_data[i]])
    for i in range(len(opt_list)):
        log_data.append([exp_group_id, exp_id, i+len(gen_list), opt_list[i], opt_input])
    print(log_data)
    print(save_logs(cursor, log_data_tab, log_data))

#Подготовка данных для передачи в процедуры оптимизации
def get_opt_input(metadata, input_data, gen_list, opt_list, cursor, proc_database):
    gen_out_opt, gen_in_opt = pydbwork.get_opt_params_ways(gen_list, opt_list, pydbwork.get_procedure_params(cursor, pydbwork.get_procedure_list(cursor, proc_database)))
    proc_num = 0
    meta = []
    for i in range(len(gen_in_opt)):
        if(gen_in_opt[i] is not None):
            b = gen_in_opt[i].keys()
            for j in b:
                proc_num = gen_list.index(j)
            c = gen_in_opt[i].values()
            for t in c:
                meta.append(input_data[proc_num][int(t)])
        else:
            meta.append(None)
    for i in range(len(gen_out_opt)):
        if(gen_out_opt[i] is not None):
            b = gen_out_opt[i].keys()
            for j in b:
                proc_num = gen_list.index(j)
            c = gen_out_opt[i].values()
            if len(metadata) > 0:
                meta[meta.index(None)] = metadata.pop()
    return meta

def get_expGroupID():
    cnxn = pydbwork.connect_db(server, analyse_database)
    cursor = cnxn.cursor()

    expGroupID = pydbwork.get_last_column_value(cursor, exp_tab, "expGroupID")+1

    cursor.close()
    cnxn.close()

    return expGroupID

#Проведение эксперемента
def make_experiment(input_data, gen_list, opt_list, server, proc_database, analyse_database,
                    expGroupID, expID, exp_name, exp_description, sol_tab, exp_tab, log_tab, log_data_tab):
    
       cnxn = pydbwork.connect_db(server, proc_database)
       cursor = cnxn.cursor()
       
       solution_and_meta, opt_input  = get_exp_solution(opt_list, gen_list, input_data, cursor, proc_database)
       meta = solution_and_meta.pop()
       solution_list = solution_and_meta
       
       print(f'solution_list: {solution_list}')
       print(f'opt_list: {opt_input}')
       
       cursor.commit()
       cursor.close()
       cnxn.close()
       
       
       cnxn =  pydbwork.connect_db(server, analyse_database)
       cursor = cnxn.cursor() 
       
       # expGroupID = pydbwo rk.get_last_column_value(cursor, exp_tab, "expGroupID")+1
       # exp_id = pydbwork.get_last_column_value(cursor, exp_tab, "expID")+1
       
       for i in range(len(solution_list)):
            # if(i==0):
           solution_list[i].append(i)
           solution_list[i].append(exp_name)
           solution_list[i].append(expID)
           solution_list[i].append(expGroupID)
           solution_list[i].reverse()
            # else:
            #     solution_list[i].append("NULL")
            #     solution_list[i].reverse()
         
       print(f'error: {solution_list}')

       exp_list = [[]]
       exp_list[0].append(expGroupID)
       exp_list[0].append(expID)
       exp_list[0].append(exp_name)
       exp_list[0].append(exp_description)
       
       save_exp_result(cursor, exp_list, solution_list, sol_tab, exp_tab)
       # add_log_inf(cursor, expGroupID, exp_id, opt_list, gen_list, log_tab)
       add_log_data(cursor, expGroupID, expID, opt_list, opt_input, input_data, gen_list, log_data_tab)
       
       cursor.commit()
       cursor.close()
       cnxn.close()
       
       return_list = []
       return_list.append(meta)
       return_list.append(input_data[0])
       # return_list.append(dimension)
        #return return_list


################################Данные с ввода
#opt_list = ['emu.OptType1Alg1', 'emu.OptType1Alg2', 'emu.OptType1Alg3']
#generation_list = ['emu.RndGenType1V1']
#input_data = [[10, 10, 30, 2]]
#dimension = [10, 10]
#exp_name = "'1Try'"
#exp_description = "'Hard'"
#################################################

##################################Данные из .ini файла
server = "SNOUBORT"
proc_database = "Emulation"
analyse_database = "SA2"

exp_tab = 'exp'
sol_tab = 'expResults'
log_tab = 'proceduresQueue'
log_data_tab = 'proceduresData'
###########################################

#Контроль кнопки начала эксперемента
def start_experement(window):
    generation_list, opt_list = window.take_lists()
    exp_name, exp_description = window.take_name_descripsion()
    input_data, iterations = window.take_table()
    print(input_data)

    for i in range(iterations):
      expID = i
      window.add_experiment (exp_name, [exp_name, exp_description, generation_list, input_data, opt_list])
    # make_experiment(input_data, generation_list, opt_list, server, proc_database, analyse_database, 
    #                     exp_name, exp_description, sol_tab, exp_tab, log_tab, log_data_tab)

#Контроль взаимодействия интерфейса с данными
def app_control(server, proc_database, analyse_database, sol_tab, exp_tab, log_tab, log_data_tab):
    
    cnxn = pydbwork.connect_db(server, proc_database)
    cursor = cnxn.cursor()
    sp_list = pydbwork.get_procedure_list(cursor, proc_database)
    sp_dict = pydbwork.get_procedure_params(cursor, sp_list)
    cursor.commit()
    cursor.close()
    cnxn.close()
    
    cnxn = pydbwork.connect_db("SNOUBORT", "SA2")
    cursor = cnxn.cursor()
    names = pydbwork.get_table_data(cursor, "exp", "Name")
    df =pd.DataFrame({"Name":names})
    names_list = []
    for i in range(len(df.Name)):
        df.loc[i, "Name"] = df.Name[i][0].strip()
    for i in df.Name:
        if(names_list.count(i) == 0):
            names_list.append(i)
            
    groupIDs = pydbwork.get_table_data(cursor, "exp", "expGroupID")
    df =pd.DataFrame({"GroupID":groupIDs})
    GroupIDs_list = []
    for i in df.GroupID:
        if(GroupIDs_list.count(str(i[0])) == 0):
            GroupIDs_list.append(str(i[0]))
    cnxn.close()

    gen_list = list(filter(lambda p: 'Gen' in p[4:], sp_list))
    opt_list = list(filter(lambda p: 'Opt' in p[4:], sp_list))
    gen_params = [[(p[0][1: ], p[1], p[3]) for p in sp_dict[procedure] if not p[2]] for procedure in gen_list]
    
    window, app = main.main(gen_list, opt_list, gen_params, GroupIDs_list, names_list)
    window.pushButton_3.clicked.connect(lambda action: start_experement(window))
    window.pushButton_6.clicked.connect(lambda action: build_graph(window))
    window.pushButton_7.clicked.connect(lambda action: do_experement(window))
    window.pushButton_8.clicked.connect(lambda action: window.del_exp_from_list())
    window.pushButton_9.clicked.connect(lambda action: build_results(window))
    
    
    window.show()  # Показываем окно
    app.exec_()  # и запускаем приложение

def build_graph(window):
    window.label_3.clear()
    window.label_4.clear()
    window.model.clear()
    
    create_graph(window)
    
    path_to_graph = "0.png"
    path_to_graph_2 = "1.png"
    window.Draw_schedule(path_to_graph, path_to_graph_2)
    window.loadCsv("3.csv", window.model)
    window.show()
    
def build_results(window):
    window.model_2.clear()
    window.model_3.clear()
    
    take_results(window)
    
    window.loadCsv("4.csv", window.model_2)
    window.loadCsv("5.csv", window.model_3)
    window.show()
    
    
def do_experement(window):
    expGroupID = get_expGroupID()
    
    if window.listWidget_3.count() == 0:
        start_experement(window)

    for i, exp in enumerate(window.exp_list):
        #exp = [name, desc, gen_list, input_data, opt_list, iterations, algID]
        
        input_data = exp[3]
        generation_list = exp[2]
        opt_list = exp[4]
        exp_name = exp[0]
        exp_description = exp[1]
        expID = i

        make_experiment(input_data, generation_list, opt_list, server, proc_database, analyse_database, 
                        expGroupID, expID, exp_name, exp_description, sol_tab, exp_tab, log_tab, log_data_tab)
        
        window.delete_experiments()
        
        cnxn = pydbwork.connect_db("SNOUBORT", "SA2")
        cursor = cnxn.cursor()
        names = pydbwork.get_table_data(cursor, "exp", "Name")
        df =pd.DataFrame({"Name":names})
        names_list = []
        for i in range(len(df.Name)):
            df.loc[i, "Name"] = df.Name[i][0].strip()
        for i in df.Name:
            if(names_list.count(i) == 0):
                names_list.append(i)
                
        groupIDs = pydbwork.get_table_data(cursor, "exp", "expGroupID")
        df =pd.DataFrame({"GroupID":groupIDs})
        GroupIDs_list = []
        for i in df.GroupID:
            if(GroupIDs_list.count(str(i[0])) == 0):
                GroupIDs_list.append(str(i[0]))
        cnxn.close()

        window.comboBox.clear()
        window.comboBox_2.clear()
        window.comboBox_3.clear()

        window.comboBox.addItems(names_list)
        window.comboBox_2.addItems(GroupIDs_list)
        window.comboBox_3.addItems(names_list)


def create_graph(window):
    GroupID, Name = window.take_Group_exp_ID()  
    cnxn = pydbwork.connect_db("SNOUBORT", "SA2")
    cursor = cnxn.cursor()
    #cursor.execute("EXECUTE [dbo].[PyPlotMatplotlib]")
    print(f"EXECUTE [dbo].[CP] {GroupID}, {Name}")
    cursor.execute(f"EXECUTE [dbo].[CP] {GroupID}, {Name}")
    tables = cursor.fetchall()
    #for i in range(0, len(tables)):
    fig = pickle.loads(tables[0][0])
    fig2 = pickle.loads(tables[1][0])
    tab = pickle.loads(tables[2][0])

    # fig.show()
    fig.savefig('0.png')
    fig2.savefig('1.png')
    tab.to_csv("3.csv", encoding='utf-8')
    
    print(tab)
    print("The plots are saved in directory: ",os.getcwd())
    
def take_results(window):
    Name = window.take_Name()  
    cnxn = pydbwork.connect_db("SNOUBORT", "SA2")
    cursor = cnxn.cursor()
    
    cursor.execute(f"EXECUTE [dbo].[DT] {Name}")
    tables = cursor.fetchall()
    tab = pickle.loads(tables[0][0])
    tab.to_csv("4.csv", encoding='utf-8')

    cursor.execute(f"EXECUTE [dbo].[IT] {Name}")
    tables = cursor.fetchall()
    tab2 = pickle.loads(tables[0][0])
    tab2.to_csv("5.csv", encoding='utf-8')
    print(tab)
    print(tab2)
    print("The plots are saved in directory: ",os.getcwd())
        

# print(make_experiment(input_data, generation_list, opt_list, server, proc_database, analyse_database, 
#                          exp_name, exp_description, sol_tab, exp_tab, log_tab, log_data_tab))

app_control(server, proc_database, analyse_database, sol_tab, exp_tab, log_tab, log_data_tab)
    
# make_experiment(input_data, generation_list, opt_list, server, proc_database, analyse_database, 
#                    exp_name, exp_description, sol_tab, exp_tab, log_tab, log_data_tab)


#Данные для быстрого восстановления базы данных
# exp
# expGroupID
# expID
# Name
# Description

# expResults
# expGroupID
# expID
# Name
# algID
# Solution
# Time

# proceduresData
# expGroupID
# expID
# runQueue
# proceduresNames
# Params