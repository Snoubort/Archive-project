# -*- coding: utf-8 -*-

import sys  # sys нужен для передачи argv в QApplication
from PyQt5 import QtWidgets, QtGui, QtCore
from PyQt5.QtGui import QPixmap
import design  # Это наш конвертированный файл дизайна
import csv

class App(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def __init__(self, gen_procedures, opt_procedures, params, GroupIDs, Names):
        # Это здесь нужно для доступа к переменным, методам
        # и т.д. в файле design.py
        super().__init__()
        self.setupUi(self)  # Это нужно для инициализации нашего дизайна
        
        self.comboBox.addItems(Names)
        self.comboBox_2.addItems(GroupIDs)
        self.comboBox_3.addItems(Names)
        
        self.gen_list = []
        self.opt_list = []
        self.gen_procedures = gen_procedures
        self.opt_procedures = opt_procedures
        self.exp_list = []

        self.param_list = []
        self.params = params
        print(params)
        
        self.fill_table()
        
        self.gen_menu_button()
        self.opt_menu_button()
        self.pushButton_4.clicked.connect(lambda action: self.removeSelGen())
        self.pushButton_5.clicked.connect(lambda action: self.removeSelOpt())
        
        
    def gen_click (self, action_text):
        self.listWidget.addItem(action_text)
        self.gen_list.append(action_text)
        #Перенёс из gen_menu_button в gen_click, поскольку после функции в 49 строке fill_table не вызывалась
        self.fill_table(action_text)
        
    def opt_click (self, action_text):
        self.listWidget_2.addItem(action_text)
        self.opt_list.append(action_text)
        
    def add_experiment (self, exp_name, full_exp):
        self.listWidget_3.addItem(exp_name)
        self.exp_list.append(full_exp)

    
    
    def gen_menu_button(self):
        menu = QtWidgets.QMenu(self)
        self.create_menu(self.gen_procedures, menu)
        self.pushButton.setMenu(menu)
        menu.triggered.connect(lambda action: self.gen_click(action.text()))
        # self.fill_table(self.param_list)
        
    def opt_menu_button(self):
        menu = QtWidgets.QMenu(self)
        self.create_menu(self.opt_procedures, menu)
        self.pushButton_2.setMenu(menu)
        menu.triggered.connect(lambda action: self.opt_click(action.text()))
        
    def create_menu(self, d, menu):
        if isinstance(d, list):
            for e in d:
                self.create_menu(e, menu)
        else:
            action = menu.addAction(d)
            action.setIconVisibleInMenu(False)
            
    def take_lists(self):
        return self.gen_list, self.opt_list
    
    def take_name_descripsion(self):
        name = self.lineEdit_2.text()
        description = self.lineEdit.text()
        
        name = f"{name}"
        description = f"{description}"
        return name, description
    
    def take_Group_exp_ID(self):
        GroupID = self.comboBox_2.currentText()
        expName = self.comboBox.currentText()
        
        return int(GroupID), expName
    
    def take_Name(self):
        expName = self.comboBox_3.currentText()
        
        return expName

    def removeSelGen(self):
        listItems=self.listWidget.selectedItems()
        if not listItems: return        
        for item in listItems:
            self.gen_list.pop(self.listWidget.row(item))
            self.listWidget.takeItem(self.listWidget.row(item))

        self.param_list = []
        if self.gen_list == []: 
            self.fill_table()
            return
        self.tableWidget.setHorizontalHeaderLabels(self.param_list)
        for procedure in self.gen_list:
            self.fill_table(procedure) 
            
    def removeSelOpt(self):
        listItems=self.listWidget_2.selectedItems()
        if not listItems: return       
        for item in listItems:
            self.opt_list.pop(self.listWidget_2.row(item))
            self.listWidget_2.takeItem(self.listWidget_2.row(item))
            
    def fill_table(self, sp_gen_name = None):
        if sp_gen_name == None: 
            self.tableWidget.setColumnCount(len(self.param_list))
            self.tableWidget.setHorizontalHeaderLabels(self.param_list)
            return
        p_list = [p[0] for p in self.params[self.gen_procedures.index(sp_gen_name)]]
        self.param_list.extend(list(filter(lambda p: p not in self.param_list, p_list)))

        self.tableWidget.setColumnCount(len(self.param_list))
        self.tableWidget.setHorizontalHeaderLabels(self.param_list)

    #Получение входных параметров эксперимента, а также числа итераций
    def take_table(self):
        input_data = []
        for i in range(self.tableWidget.rowCount()):
            input_data.append([])
            print(f'j: {self.tableWidget.columnCount} {len(self.param_list)}')
            print(f'i: {self.tableWidget.rowCount}')
            for j in range(len(self.param_list)):
                input_data[i].append(self.get_converted_value(self.param_list[j], self.tableWidget.item(i, j).text()))
        iterations = int(self.lineEdit_5.text())

        self.clear_fields()

        return input_data, iterations

    #Конвертация значения параметра к соответствующему типу данных параметра ХП
    def get_converted_value(self, p_name, p_value):
        param_tuple = [[p for p in p_list if p[0] == p_name] for p_list in self.params][0][0]
        print('\n\n', param_tuple)
        if param_tuple[1] in ['char', 'varchar', 'nchar', 'nvarchar']:
            if len(p_value) > param_tuple[2]:
                return str(p_value)[:param_tuple[2]]
            return str(p_value)
        print(param_tuple[1], ' ', type(param_tuple[1]))
        result = eval(f"{param_tuple[1]}({p_value})")
        print(result, ' ', type(result))
        return result
            
    def Draw_schedule(self, path_to_graph_1, path_to_graph_2):
        pixmap = QPixmap(path_to_graph_1)
        self.label_3.setPixmap(pixmap)
        pixmap = QPixmap(path_to_graph_2)
        self.label_4.setPixmap(pixmap)
        
    def loadCsv(self, fileName, model):
        with open(fileName, "r") as fileInput:
            for row in csv.reader(fileInput):    
                items = [
                    QtGui.QStandardItem(field)
                    for field in row
                ]
                model.appendRow(items)
                
    def clear_model(self):
        self.tree.setModel(None)
        
    def clear_fields(self):
        self.listWidget.clear()
        self.listWidget_2.clear()
        self.gen_list = []
        self.opt_list = []
        self.param_list = []
        self.fill_table()
        
    def delete_experiments(self, index = None):
        if index == None:
            self.exp_list = []
            self.listWidget_3.clear()
            return
        self.exp_list.pop(index)
        self.listWidget_3.takeItem(index)
        
    def del_exp_from_list(self):
        listItems = self.listWidget_3.selectedItems()
        if not listItems: return
        for item in listItems:
            index = self.listWidget_3.row(item)
            self.delete_experiments(index)



def main(gen_procedures, opt_procedures, params, GroupIDs, Names):
    app = QtWidgets.QApplication(sys.argv)  # Новый экземпляр QApplication
    window = App(gen_procedures, opt_procedures, params, GroupIDs, Names)  # Создаём объект класса App
    return window, app
    # window.show()  # Показываем окно
    # app.exec_()  # и запускаем приложение

# if __name__ == '__main__':  # Если мы запускаем файл напрямую, а не импортируем
#     main()  # то запускаем функцию main()