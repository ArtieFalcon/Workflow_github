import ipywidgets as widgets
from IPython.display import HTML, display, clear_output
from ipywidgets import Button, Layout, HBox, VBox
import subprocess  # для запуска bash-комманд
import inspect  # для функции получения сырого кода из определения функции

class CellRunner():
    """
    Класс для запуска ячейки с номером index_of_cell после текущей из ноутбука
    Необходим для автоматического запуска интерфейса, который находится в следующей ячейке
    """

    def runner(self, index_of_cell):
        self.index_of_cell = index_of_cell
        self.js_command = '''
var output_area = this;
var cell_element = output_area.element.parents('.cell');
var cell_idx = Jupyter.notebook.get_cell_elements().index(cell_element);
Jupyter.notebook.execute_cells([cell_idx+{action}]);
'''.format(action=str(self.index_of_cell))
        return self.js_command

# Создание HTML объектов (глобальных переменных), которые будут доступны в нескольких классах

button_write = Button(description="Проверить", button_style='info', icon='check', tooltip='Для отправки ответа нажмите на кнопку', layout=Layout(width='255px', height='47px', border='1px solid #0d4282'))

button_show_solution = Button(description="Показать решение", button_style='warning', layout=Layout(width='155px', height='30px', border='1px solid #0d4282'))

button_tip = Button(description="Подсказка", button_style='primary', tooltip='После проверки задания станет доступным просмотр решения', layout=Layout(width='135px', height='30px', border='1px solid #0d4282'))

html_empty = '''<button class="button-93" role="button"> Нажмите проверить</button>'''
css_empty = '''
<style> /* CSS */
.button-93 {  color: #3356D7;  background: #FFFFFF;  border: 1px solid #FFFFFF; text-align: left;
  text-decoration: none;  user-select: none;  -webkit-user-select: none;  touch-action: manipulation;
  white-space: nowrap;  cursor: pointer;  width: 176px;  height: 45px;  font-size: 14px !important;  }
</style>'''
button_empty = widgets.HTML(value=html_empty + css_empty)

html_correct = '''<button class="button-93" role="button"> Верно!</button>'''
button_correct = widgets.HTML(value=html_correct + css_empty)
html_incorrect = '''<button class="button-93" role="button"> Неверно. Попробуй снова.</button>'''
button_incorrect = widgets.HTML(value=html_incorrect + css_empty)

box_layout = widgets.Layout(display='flex',
                            flex_flow='column',
                            align_items='flex-start',  # 'stretch',
                            width='250px')

# HTML-кнопка-ключ, которая позволяет скрывать содержимое ячейки, то есть скрывать сырой код бекенда от студента
HH = HTML('''<script>
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


class HomeTaskInterface:
    # Класс для Unit-тестов (при вызове интерфейса в ноутбуках)

    def __init__(self, task_number: int, result):

        self.result = result
        self.task_number = task_number

        # Определение ответов и подсказок в зависимости от номера задачи
        if self.task_number == 1:
            self.answer = "headman = names_list[0]"
            self.tip = 'Обратите внимание, что первый элемент в списке имеет индекс = 0'
        elif self.task_number == 2:
            self.answer = "names_list_1 = names_list[::2]"
            self.tip = 'Используйте slice (срез) с шагом = 2. Помните, что первый элемент в списке имеет индекс = 0'
        elif self.task_number == 3:
            self.answer = "names_list_reverse = names_list[::-1]"
            self.tip = 'Используйте slice (срез) с шагом = -1'
        elif self.task_number == 4:
            self.answer = "love_list = names_list[1:4:2]"
            self.tip = '''Измените содержимое скобок, а сами скобки должны быть квадратными []. 
Используйте срезы и шаг внутри квадратных скобок. 
В списке должна быть Маша и Сергей.'''
        elif self.task_number == 5:
            self.answer = '''male_set = {'Александр', 'Сергей', 'Константин', 'Максим', 'Роман', 'Олег'}
female_set = {'Алла', 'Маша', 'Вера', 'Елена'}
dict_gender = {"М": male_set, "Ж": female_set}'''
            self.tip = ''' Проверь, что ключи словаря  - буквы в русской раскладке (М - должна быть русской).
Проверь, что ключи словаря - заглавные буквы М, Ж.
Порядок следования имен в множествах неважен. Поэтому проверка задания безразлична к порядку следования имен в множествах male_set и female_set.  '''

        # Переопределение html объектов
        self.button_write = button_write
        self.button_show_solution = button_show_solution
        self.button_tip = button_tip
        self.html_empty = html_empty
        self.css_empty = css_empty
        self.button_empty = button_empty
        self.html_correct = html_correct
        self.button_correct = button_correct
        self.html_incorrect = html_incorrect
        self.button_incorrect = button_incorrect

        self.box_layout = box_layout
        self.HH = HH

    # Определяем функции - действия при нажатии на кнопки. Переменная a -обязательна, является обьектом-кнопкой.
    # Всего два вида получения ответа - переменная и функция. Для ответов, где необходимо сделать выбор - отдельный класс ниже.

    # Функция для проверки Переменной result
    def write_result_var(self, result):
        try:
            with open('lib_hometask/Unit_test_Home_task.py', 'w') as f:
                # Функции нужно распарсить, а переменные передаем как есть в виде string
                if callable(self.result):  # Если result - это функция
                    func_raw_code = str(inspect.getsource(self.result))  # забираем функцию как строку
                    func_raw_code = func_raw_code.split('\n')  # парсим строку, потому что функция getsource удаляет табуляцию на вторых строках
                    for i in range(1, len(func_raw_code) - 1):
                        func_raw_code[i] = str('\n\t' + func_raw_code[i])  # добавляем всем строкам табуляцию, кроме первой
                    # Добавляем "self" для превращения функции в метод
                    if 'def ' in func_raw_code[0]:  # Если в первой строке ключевое слово def
                        splitted_row_right = func_raw_code[0].split('(')[1]  # смотрим то что справа от скобки
                        # необходимо понять было ли что-то в скобках (переменные)
                        if any(c.isalpha() for c in splitted_row_right.split(')')[0]):  # если в скобках что то было
                            splitted_row_right = '(self, ' + splitted_row_right  # добавляем self с запятой
                        else:
                            splitted_row_right = '(self' + splitted_row_right  # если переменных не было - без запятой
                    func_raw_code[0] = func_raw_code[0].split('(')[0] + splitted_row_right  # соединяем левую от скобки часть (т.е. [0]) и созданную правую часть
                    func_raw_code_str = str(''.join(func_raw_code))  # соединяем список строк с новыми табуляциями
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
                '''.format(task_number=self.task_number, resp='\"' + str(self.result) + '\"')

                f.write(class_str)

                print('Ответ принят: ', self.result)

        except NameError:
            print("Сначала запустите ячейку с ответом, затем снова отправьте ответ")

# Непосредственно проверка:
        print('Проверка началась. Ожидайте результат')
        output = subprocess.run('python3 lib_hometask/Test_Unit_test_task1.py ', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')  # .stdout
        if 's\n\nOK' in output.stderr:
            clear_output(wait=True)  # очиска предыдущего output ячейки
            display(HBox([self.button_write, VBox([self.button_correct], layout=self.box_layout), self.button_tip,
                  self.button_show_solution]))
            display(self.HH)
            print(output.stderr)  # именно атрибут stderr, т.к. output.stdout - не выведет результат.
    # display(HTML('<h2>Ответ ВЕРНЫЙ! Отлично!</h2>'))
        else:
            clear_output(wait=True)  # очиска предыдущего output ячейки
            display(HBox([self.button_write, VBox([self.button_incorrect], layout=self.box_layout), self.button_tip, self.button_show_solution]))
            display(self.HH)
            print(output.stderr)
            # print('Ответ НЕВЕРНЫЙ, попробуй подсказку. Также можно посмотреть решение.')

            # Определяем функции - действия при нажатии на кнопки. Переменная a -обязательна, является обьектом-кнопкой
    def on_button_show_solution_func(self, a):
        clear_output(wait=True)  # очиска предыдущего output ячейки
        self.display_hbox()  # Показать исходные кнопки
        ss = '''Решение задачи: \n''' + self.answer
        print(ss)

        # Вызов подсказки
    def on_button_tip_func(self, a):
        clear_output(wait=True)  # очиска предыдущего output ячейки
        self.display_hbox()  # Показать исходные кнопки
        sss = ''' Подсказка:\n''' + self.tip
        print(sss)

            # Показать кнопки в определенном Hbox порядке

    def display_hbox(self):

        self.button_tip.on_click(self.on_button_tip_func)
        self.button_show_solution.on_click(self.on_button_show_solution_func)
        self.button_write.on_click(self.write_result_var)

        display(HBox([self.button_write, VBox([self.button_empty], layout=self.box_layout), self.button_tip]))
        display(self.HH)

class HomeTaskInterfaceSelection:
    # Отдельный класс "Selection" для проверки ответов с несколькими вариантами.

    def __init__(self, task_number: int):
        self.task_number = task_number



        #Определение ответов и подсказок в зависимости от номера задачи
        if self.task_number == 6:
            self.selector = widgets.RadioButtons(
                options=['Списки', 'Множества', 'Словари', 'Числа'],
                value=None,
                description='',
                disabled=False
            )
            self.answer = "Числа являются неизменяемым типом"
            self.tip = 'Необходимо выбрать один НЕИЗМЕНЯЕМЫЙ тип данных'
        elif self.task_number == 7:
            self.answer = "Ответ7"
            self.tip = 'Подсказка7'

    #Определяем функции - действия при нажатии на кнопки. Переменная a -обязательна, является обьектом-кнопкой
    def on_button_show_solution_func(self, a):
        # Функция при нажатии на кнопку "Показать решение"
        clear_output(wait=True) #очиска предыдущего output ячейки
        self.display_hbox_selection() #Показать исходные кнопки
        ss = '''Решение задачи: \n''' + self.answer
        print(ss)

    #Вызов подсказки
    def on_button_tip_func(self, a):
        # Функция при нажатии на кнопку "Подсказка"
        clear_output(wait=True) #очиска предыдущего output ячейки
        #self.display_hbox_selection() #Показать исходные кнопки
        self.button_tip = button_tip
        display(self.selector)
        display(HBox([self.button_write,VBox([self.button_empty], layout=self.box_layout), self.button_tip, self.button_show_solution]))
        display(self.HH)
        sss = ''' Подсказка:\n''' + self.tip
        print(sss)

    def write_result_var(self, a):  # Функция при нажатии на кнопку "Проверить"
        selection = self.selector.get_interact_value()
        self.button_correct = button_correct
        self.button_incorrect = button_incorrect
        if (selection == 'Числа'):
            clear_output(wait=True)  # очиска предыдущего output ячейки
            # print("Correct!")
            display(self.selector)
            display(HBox([self.button_write, VBox([self.button_correct], layout=self.box_layout), self.button_tip]))
            display(self.HH)
        else:
            clear_output(wait=True)  # очиска предыдущего output ячейки
            display(self.selector)
            display(HBox([self.button_write, VBox([self.button_incorrect], layout=self.box_layout), self.button_tip]))
            display(self.HH)
            # print("Try again...")
        # self.selector.value = None   #Если необходимо "стереть" предыдущий выбор

    def display_hbox_selection(self):  # Функция - вызов интерфейса
        # Переопределение HTML объектов
        self.button_tip = button_tip
        self.button_show_solution = button_show_solution
        self.button_write = button_write
        self.button_empty = button_empty
        self.box_layout = box_layout
        self.HH = HH
        # Определяем функции при нажатии на кнопки
        self.button_tip.on_click(self.on_button_tip_func)
        self.button_show_solution.on_click(self.on_button_show_solution_func)
        self.button_write.on_click(self.write_result_var)
        # Отображение интерфейса
        display(self.selector)
        display(HBox([self.button_write, VBox([self.button_empty], layout=self.box_layout), self.button_tip]))
        display(self.HH)
