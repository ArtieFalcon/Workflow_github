import ipywidgets as widgets
from IPython.display import HTML, display, clear_output
from ipywidgets import Button, Layout, HBox, VBox
import subprocess #для запуска bash-комманд
import inspect #для функции получения сырого кода из определения функции

class CellRunner():
        # Класс для запуска ячейки с номером index_of_cell после текущей из ноутбука
        # Необходим для автоматического запуска интерфейса, который находится в следующей ячейке
        
    def runner(self, index_of_cell):
        self.index_of_cell = index_of_cell
        self.js_command ='''
var output_area = this;
var cell_element = output_area.element.parents('.cell');
var cell_idx = Jupyter.notebook.get_cell_elements().index(cell_element);
Jupyter.notebook.execute_cells([cell_idx+{action}]);
'''.format(action = str(self.index_of_cell))
        return self.js_command

  
class HomeTaskInterface:
   #Класс для вызова интерфейса в ноутбуках при выполнении домашних заданий
           
    def __init__(self, task_number: int, result): 
        
        self.result = result
        self.task_number = task_number
        
        #Определение ответов и подсказок в зависимости от номера задачи
        if self.task_number == 1:
            self.answer = "answer 1"  
            self.tip = 'tip 1'
        elif self.task_number == 2:
            self.answer = "answer 2"  
            self.tip = 'tip 2'
        elif self.task_number == 3:
            self.answer = "answer 3"
            self.tip = 'tip 3'
            
            
        #Создание кнопок
        # Для выбора цвета из палитры введите hex-code цвета (Google: 'hex color')
        self.button_write = Button(description="ОТПРАВИТЬ ОТВЕТ", button_style='info', icon='check', tooltip='Для отправки ответа нажмите на кнопку', layout=Layout(width='255px', height='47px', border='1px solid #0d4282'))    
        self.button_show_solution = Button(description="Показать решение", button_style= 'warning', layout=Layout(width='135px', height='30px', border='1px solid #0d4282'))
        self.button_feedback = Button(description="Обратная связь", button_style='primary',tooltip='Технические вопросы и пожелания', layout=Layout(width='135px', height='30px', border='1px solid #0d4282'))
        self.button_check_unittest = Button(description="ПРОВЕРИТЬ ЗАДАНИЕ", button_style='success',tooltip='Для проверки ответа нажмите на кнопку', layout=Layout(width='255px', height='47px', border='1px solid #0d4282'))
        self.button_clear =  Button(description="Стереть результат ", button_style='primary', tooltip='Удалить output', layout=Layout(width='135px', height='30px', border='1px solid #0d4282'))
        self.button_tip = Button(description="Подсказка", button_style='primary', tooltip='После проверки задания станет доступным просмотр  решения', layout=Layout(width='135px', height='30px', border='1px solid #0d4282'))
        self.button_correct_answer = Button(description="ОТВЕТ ВЕРНЫЙ!", button_style='success', tooltip='Молодец!', layout=Layout(width='335px', height='98px', border='1px solid #0d4282'))
        self.button_incorrect_answer = Button(description="ОТВЕТ НЕВЕРНЫЙ Попробуй еще раз", button_style='danger', tooltip='Попробуй посмотреть подсказку', layout=Layout(width='335px', height='98px', border='1px solid #0d4282'))
        
        #Создание описаний
        self.html_instruction_desc = '''
<form action="/html/tags/html_form_tag_action.cfm" method="post">
<textarea name="comments" id="comments" disabled style="font-family:Times New Roman; font-size: 15.5px; line-height: 14px;  width:392px;
padding:1px;height:98px;
background-color:#ebf2ff;border:5px groove #4383fa;">
Инструкция:
1. Нажмите на кнопку "ОТПРАВИТЬ ОТВЕТ"
2. Дождитесь фразы 'Ответ принят'.
3. Нажмите на кнопку 'ПРОВЕРИТЬ ЗАДАНИЕ'.
Ответ можно переотправлять.
Будет принят последний из отправленных.
</textarea><br>
</form>
'''
        #HTML(html_desc)
        self.label_instruction = widgets.HTML(value=self.html_instruction_desc)
        
        #Создание описаний
        self.caption0 = widgets.Label(value='''Дождитесь фразы 'Ответ принят'. Затем нажмите на кнопку 'Проверить'. Ответ можно переотправлять. Будет     принят последний из отправленных.''')
        
        self.button_empty = Button(description= 'desc1', button_style='', tooltip='Инструкция', layout=Layout(width='335px', height='98px', border='1px solid #0d4282'))


        
        
        # HTML-кнопка-ключ, которая позволяет скрывать содержимое ячейки, то есть скрывать сырой код бекенда от студента 
        self.HH = HTML('''<script>
        code_show=true; 
        function code_toggle() {
         if (code_show){
         $('div.cell.code_cell.rendered.selected div.input').hide();
         } else {
         $('div.cell.code_cell.rendered.selected div.input').show();
         }
         code_show = !code_show
        } 
        $( document ).ready(code_toggle);
        </script>
        <form action="javascript:code_toggle()"  align="right"><input type="submit" value="Raw code"> </form>''')
        
        #Ответсвенные за доработку\ контакты для обратной связи
        self.responsibles = 'Пащенко Данил, Соколов Артемий'
    
    #Определяем функции - действия при нажатии на кнопки. Переменная a -обязательна, является обьектом-кнопкой.
    #Всего два вида получения ответа - переменная и функция
    
    #Функция для проверки Переменной result
    def write_result_var(self, result):
        try:
            with open('Unit_test_Home_task.py', 'w') as f:
                #Функции нужно распарсить, а переменные передаем как есть в виде string
                if callable(self.result): # Если result - это функция
                    func_raw_code = str(inspect.getsource(self.result)) # забираем функцию как строку
                    func_raw_code = func_raw_code.split('\n') # парсим строку, потому что функция getsource удаляет табуляцию на вторых строках
                    for i in range(1,len(func_raw_code)-1):
                        func_raw_code[i] = str('\n\t' + func_raw_code[i] ) #добавляем всем строкам табуляцию, кроме первой
                    #Добавляем "self" для превращения функции в метод    
                    if 'def ' in func_raw_code[0]: #Если в первой строке ключевое слово def
                        splitted_row_right = func_raw_code[0].split('(')[1] #смотрим то что справа от скобки
                        #необходимо понять было ли что-то в скобках (переменные)
                        if any(c.isalpha() for c in splitted_row_right.split(')')[0]): #если в скобках что то было
                            splitted_row_right = '(self, ' + splitted_row_right #добавляем self с запятой
                        else:
                            splitted_row_right = '(self' + splitted_row_right #если переменных не было - без запятой
                    func_raw_code[0] = func_raw_code[0].split('(')[0] + splitted_row_right #соединяем левую от скобки часть (т.е. [0]) и созданную правую часть
                    func_raw_code_str = str(''.join(func_raw_code)) #соединяем список строк с новыми табуляциями
                    class_str = '''
class Solution:
    def __init__(self):
        pass          
    ''' + func_raw_code_str
                    
                else:
                    class_str = '''
class Solution:
    def __init__(self):
        pass
    def check{task_number}(self):
        return {resp}
        '''.format(task_number = self.task_number, resp = '\"' + str(self.result) + '\"')
                    
                f.write(class_str)
                
                clear_output(wait=True) #очиска предыдущего output ячейки
                display(HBox([VBox([self.button_write, self.button_check_unittest]),VBox([self.button_tip, self.button_feedback, self.button_clear]), self.label_instruction]))
                display(self.HH)
                print('Ответ принят: ', self.result)
                
        except NameError:
            clear_output(wait=True) #очиска предыдущего output ячейки
            display(self.button_write)
            display(HBox([self.button_check_unittest, self.button_tip, self.button_feedback, self.button_clear]))
            print("Сначала запустите ячейку с ответом, затем снова отправьте ответ")  
            display(self.HH)
      
    
    #Определяем функции - действия при нажатии на кнопки. Переменная a -обязательна, является обьектом-кнопкой
    def on_button_show_solution_func(self, a): 
        clear_output(wait=True) #очиска предыдущего output ячейки
        self.display_hbox() #Показать исходные кнопки
        ss = '''Решение задачи: \n''' + self.answer
        print(ss)

    #Для обратной связи
    def on_button_feedback_func(self, a):
        print('''Для обратной связи свяжитесь c: {0}'''.format(self.responsibles))

    #Проверка. Запуск файла с Unit тестом с помощью bash-команды
    def on_button_check_unittest_func(self, a):
        clear_output(wait=True) #очиска предыдущего output ячейки
        print('Проверка началась. Ожидайте результат')
        #exec('!python Test_Unit_test_task1.py -v') #ais поменять название в исходнике # -v это детально по каждому методу
        output = subprocess.run('python3 Test_Unit_test_task1.py ', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8') #.stdout 
        if 's\n\nOK' in output.stderr:
            display(HBox([VBox([self.button_write, self.button_check_unittest]),VBox([self.button_show_solution, self.button_feedback, self.button_clear]), self.button_correct_answer]))
            display(self.HH)
            print(output.stderr) #именно атрибут stderr, т.к. output.stdout - не выведет результат.
            #display(HTML('<h2>Ответ ВЕРНЫЙ! Отлично!</h2>'))

        else:
            
            display(HBox([VBox([self.button_write, self.button_check_unittest]),VBox([self.button_show_solution, self.button_feedback, self.button_clear]), self.button_incorrect_answer]))
            display(self.HH)
            print(output.stderr)
            #print('Ответ НЕВЕРНЫЙ, попробуй подсказку. Также можно посмотреть решение.')
            
    
    def on_button_clear_func(self, a):
        clear_output(wait=True) #очиска предыдущего output ячейки
        self.display_hbox() #Показать исходные кнопки
    
    #Вызов подсказки
    def on_button_tip_func(self, a):
        sss = ''' Подсказка:\n''' + self.tip 
        print(sss)   
        
    
    #Показать кнопки в определенном Hbox порядке
    def display_hbox(self):
        
        button_tip = self.button_tip
        button_tip.on_click(self.on_button_tip_func)
       
        self.button_show_solution.on_click(self.on_button_show_solution_func)
        self.button_feedback.on_click(self.on_button_feedback_func) 
        self.button_check_unittest.on_click(self.on_button_check_unittest_func) 
        self.button_clear.on_click(self.on_button_clear_func)
        self.button_write.on_click(self.write_result_var)

        #display(self.caption0)
        #display(self.html_desc)
        #display(HBox([self.button_write, self.button_tip, self.button_feedback, self.button_clear]))
        display(HBox([VBox([self.button_write, self.button_check_unittest]),VBox([self.button_tip, self.button_feedback, self.button_clear]), self.label_instruction]))
        
        # HTML-кнопка-ключ, которая позволяет скрывать содержимое ячейки, то есть скрывать сырой код бекенда от студента 
        display(self.HH)