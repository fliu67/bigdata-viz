#!/usr/bin/env python
# -*- coding: utf-8 -*-



import re
import sys
import getopt
import os
import shutil
import graph
import pprint

# json文件
file_name = None
# 文件内容
content = None

file = None

output_type = ''


begincode_pyspark = ['#!/usr/bin/env python','import sys', '# -*- coding: utf-8 -*-', 'from pyspark import SparkConf , SparkContext','conf = SparkConf().setMaster("local").setAppName("My App")', 'sc = SparkContext(conf = conf)','def main():']

begincode_python_1 = ['#!/usr/bin/env python','import sys', '# -*- coding: utf-8 -*-']

begincode_python_2 = ['def main():']

blank = '   '

shell_command = []

assignment_statement = ['=','+','!','-']

pyspark_submit = 'spark-submit --master yarn '

python_submit = 'python '

endcode = ['if __name__ == "__main__":','   main()']

paragraph_id = 'default'

file_type ={'python': 'py','pyspark': 'py', 'sh': 'sh', 'spark': 'scala', 'angular':'js'}

interpreter = ['pyspark','sh','spark','angular','python']

generate_type = []

generate_id = []

All_Variables = {}

All_Functions = {}

Function_Contents = {}

Var_Values = {}

pycode = []

initial_graph = []

DependencyGraph = graph.Graph(initial_graph , directed=True)





class Output(object):

    # 判断是否是空白字符
    def is_blank(self, index):
        if content[index] == '\n':
            self.linenum += 1
        return content[index] == ' ' or content[index] == '\t' or content[index] == '\n' or content[index] == '\r'

    # 跳过空白字符
    def skip_blank(self, index):
        while index < len(content) and self.is_blank(index):
            index += 1
        return index

    def delete_duplicated_element_from_list(self,listA):
        return sorted(set(listA), key=listA.index)


    # 打印
    # def print_log(self, style, value):
    #     print '(%s, %s)' % (style, value)

    # 判断是否是关键字
    def is_interpreter(self, value):
        for item in interpreter:
            if value in item:
                return True
        return False

    def test(self):
        print(code)
        print("\n")
        print(i)
        print("\n")

    def export_var_func(self):
        sh_file = open( 'Varieties.txt', 'w')
        for key, value in All_Variables.items():
            sh_file.write(key)
            sh_file.write(':')
            for item in value:
                sh_file.write(item)
                sh_file.write(';')
            sh_file.write('\n')
        sh_file = open('Functions.txt', 'w')
        for key, value in All_Functions.items():
            sh_file.write(key)
            sh_file.write(':')
            for item in value:
                sh_file.write(item)
                sh_file.write(';')
            sh_file.write('\n')






    # 主程序
    def main(self):
        code = []
        command = ''

        i = 0
        flag = 0
        line = ''
        while i < len(content):
            #i = self.skip_blank(i)
            #print(i)
            #if flag == 1:
            #   break

            if (content[i] == '\"') and (content[i + 1] == 't') and (content[i + 2] == 'e') and (content[i + 3] == 'x')  and (content[i + 4] == 't') and (content[i + 5] == '\"')and (content[i + 6] == ':'):
                    #判断出text关键字"

                    print("Find text")
                    flag = 0
                    global output_type

                    function_generate_code = []
                    function_generate = [] #Save the name of function that are need to be generated in the script
                    code = []
                    variable = []
                    function = []
                    dependency = []


                    i += 9

                    type_temp = ''
                    while i < len(content) and (content[i].isalpha()):
                        type_temp += content[i]
                        i += 1
                    #print (type_temp)

                    #print (type_temp)
                    if self.is_interpreter(type_temp):
                        output_type = file_type[type_temp]
                        delete_type = output_type
                    print(output_type)

                    #i += 16
                    #跳过pyspark
                    
                    #代码部分
                    while i < len(content):

                        # flag_n = 1;
                        # if content[i] == '\\' :
                        #    flag_n == -flag_n

                        #print(i)
                        if content[i] == '\"' :

                            #print("End of Code")
                            #print(i)
                            if output_type == 'py':
                                line = blank + line

                            code.append(line)
                            # if output_type == 'sh':
                            #     shell_command.append(line)

                            line = ''
                            #print(code)
                            print("___________________________END_______________________________")
                            #flag = 1
                            break

                        elif (content[i] == '\\') and content[i + 1] == '\"':
                            #print("Find\ & \"")


                            i += 1
                            #print(i)
                            #print(content[i])
                            line += content[i]
                            i += 1
                            #print(line)
                        elif content[i] == '\\' and content[i + 1] == 'n':
                            if content[i-1] != '\\' :
                                #print("Find \n")
                                line += ''

                                i += 2
                                #print(i)
                                #print(content[i])
                                if output_type == 'py':
                                    line = blank + line

                                code.append(line)

                                # if output_type == 'sh':
                                #     shell_command.append(line)

                                line = ''
                                #print(code)
                            else :
                                i += 2
                                line += 'n'




                        ###########dependency part##############
                        #####find var#####
                        elif output_type == 'py' and content[i] == '=' and  content[i - 1] not in assignment_statement and content[i + 1] not in assignment_statement:
                            line += content[i]

                            j = 1
                            k = 1
                            while not content[i - j].isalpha():
                                #print content[i - j]
                                #print line
                                j += 1
                            while content[i - j - k].isalpha() and not (content[i - j - k] == 'n' and content[i - j - k - 1] == '\\'):
                                k += 1
                            variable_temp = ''
                            for x in range(1, k + 1):
                                variable_temp += content[i - j - k + x]

                            flag_find_dependency = 0;
                            for key, value in All_Variables.items():
                                if variable_temp in value:
                                    print('Variables in Current to Paragraph ' + key)
                                    dependency.append(key)
                                    flag_find_dependency = 1;
                            if flag_find_dependency == 0:
                                variable.append(variable_temp)
                                Var_Values[variable_temp] = 0




                            i += 1

                        #####find def######
                        elif output_type == 'py' and content[i] == 'd' and content[i + 1] == 'e' and content[i + 2] == 'f' and content[i + 3] == ' ':
                            i += 3
                            line += 'def '
                            function_defined = []
                            i = self.skip_blank(i)
                            def_func_temp = ''
                            while content[i].isalpha():
                                def_func_temp += content[i]
                                i += 1
                            function.append(def_func_temp)
                            line += def_func_temp

                            ############
                            while content[i] != ':':
                                line += content[i]
                                i += 1
                            i += 1
                            line += ':'
                            function_defined.append(line)
                            line = blank + line
                            code.append(line)
                            line = ''

                            flag_end_of_func = 0
                            while flag_end_of_func == 0:
                                if content[i] == '\\' and content[i + 1] == 'n':
                                    if content[i + 2].isalpha():
                                        print line
                                        flag_end_of_func = 1
                                        print ("end of func")
                                        # print flag_end_of_func
                                    function_defined.append(line)
                                    print function_defined
                                    line = blank + line
                                    i += 2
                                    # print line
                                    code.append(line)
                                    line = ''
                                else:
                                    line += content[i]
                                    i += 1
                            Function_Contents[def_func_temp] = function_defined

                        #####find func#####
                        elif output_type == 'py' and content[i] == '(' :
                            line += content[i]
                            j = 1
                            k = 1
                            while not content[i - j].isalpha():
                                # print content[i - j]
                                # print line
                                j += 1
                            while content[i - j - k].isalpha() and not (
                                    content[i - j - k] == 'n' and content[i - j - k - 1] == '\\'):
                                k += 1
                            function_temp = ''
                            for x in range(1, k + 1):
                                function_temp += content[i - j - k + x]
                            flag_find_dependency = 0;
                            for key, value in All_Functions.items():
                                if function_temp in value:
                                    print('Functions in Current to Paragraph ' + key)
                                    dependency.append(key)

                                    function_code = Function_Contents[function_temp]
                                    function_generate_code[len(function_generate_code):len(function_code)] = function_code

                                    function_generate.append(function_temp)
                                    flag_find_dependency = 1;

                            i += 1



                        ######################################3##

                        else:
                            line += content[i]
                            i += 1

                        ###########dependency part##############

                        ######################################3##



                    #Merge Python Code

                    # if output_type == 'py' and type_temp == 'python':
                    #     pycode[len(pycode):len(code)] = code
                    #     code = pycode[:]
                    #     print(pycode)


                    #ID 部分
                    while i < len(content) and flag != 1:
                        if  (content[i] == '\"') and  (content[i + 1] == 'i') and (content[i + 2] == 'd') and (content[i + 3] == '\"') and (content[i + 4] == ':'):
                            i += 6
                            global paragraph_id
                            paragraph_id = ''
                            while i < len(content):
                                #print(i)
                                if content[i] == '\"':
                                    print("End of ID")

                                    #print(i)

                                    delete_id = paragraph_id

                                    print(paragraph_id)

                                    ##################
                                    # if output_type == 'py':
                                    #     sh_file = open(paragraph_id+'.txt', 'w')
                                    #     for lines in variable:
                                    #         sh_file.write(lines)
                                    #         sh_file.write('\n')
                                    #     sh_file.close()

                                    ##################

                                    #######Save Variables###########
                                    # 将本条para中所有的Variable&Function存入Dict
                                    All_Variables[paragraph_id] = variable
                                    All_Functions[paragraph_id] = function

                                    #print All_Variables
                                    #去重
                                    dependency = self.delete_duplicated_element_from_list(dependency)
                                    for nodes in dependency:
                                        DependencyGraph.add(paragraph_id , nodes)






                                    ################################



                                    if output_type == 'py' and type_temp == 'pyspark':

                                        command = pyspark_submit + paragraph_id + '.py'
                                        shell_command.append(command)

                                    elif output_type == 'py' and type_temp == 'python':
                                        command = python_submit + paragraph_id + '.py'
                                        shell_command.append(command)

                                    elif output_type == 'sh':

                                        command = "./" + paragraph_id + '.sh'
                                        shell_command.append(command)
                                    #     sh_file = open('command.sh', 'w+')
                                    #     sh_file.write(command)
                                    #     sh_file.write('\n')
                                    #     sh_file.close()
                                    #     print('写入sh' + paragraph_id)
                                        #command = ''

                                    #if output_type == 'sh':



                                    flag = 1
                                    break

                                if (content[i] == '\\') and content[i + 1] == '\"':
                                    #print("Find\ & \"")

                                    i += 1
                                    #print(i)
                                    #print(content[i])
                                    paragraph_id += content[i]
                                    i += 1
                                    #print(line)

                                else:
                                    paragraph_id += content[i]
                                    i += 1
                        else:
                            i += 1


                    #save file


                    # sh_file = open('command.sh', 'w')
                    # for lines in shell_command:
                    #     sh_file.write(lines)
                    #     sh_file.write('\n')
                    # sh_file.close()



                        #command[0:0] = pyspark_submit

                    # elif output_type == 'sh':
                    #     command = code
                    #
                    # shell_command.append(command)
                    # print (code)
                    # print (pycode)



                    result = open(paragraph_id + '.' + output_type, 'w')

                    if output_type == 'py':

                            #code = pycode
                            if type_temp == 'pyspark':
                                code[0:0] = begincode_pyspark
                            elif type_temp == 'python':
                                code[0:0] = begincode_python_2
                                code[0:0] = function_generate_code
                                code[0:0] = begincode_python_1

                            code[len(code):len(endcode)] = endcode

                    #print (code)
                    #print (pycode)
                        # open for 'w'riting
                    for lines in code:
                        result.write(lines)
                                # 写入换行
                        result.write('\n')
                                # write text to file
                    result.close()  # close the file

                    # print (code)
                    # print (pycode)

                    code = []




                    #elif  output_type == 'sh':

                    # result = open(paragraph_id + '.' + output_type, 'w')
                    #print(code)
                    # open for 'w'riting
                    # for lines in code:
                    #
                    #     result.write(lines)
                    #     # 写入换行
                    #     result.write('\n')
                    #     # write text to file
                    # result.close()  # close the file

                    #置空 code 和 command


            else:
                i += 1

                # save file

        #shutil.rmtree(Users/apple/Desktop/json_outdelete_id + '.' + delete_type)



        sh_file = open('command.sh', 'w')
        for commandlines in shell_command:
            sh_file.write(commandlines)
            sh_file.write('\n')
        sh_file.close()
        #保存变量和函数
        self.export_var_func()
        # pprint(Function_Contents)
        print Function_Contents

        pprint.pprint(DependencyGraph._graph)
        #pprint.pprint(All_Variables)


        # elif  output_type == 'sh':

        # result = open(paragraph_id + '.' + output_type, 'w')
        # # print(code)
        # # open for 'w'riting
        # for lines in code:
        #     result.write(lines)
        #         # 写入换行
        #     result.write('\n')
        #         # write text to file
        # result.close()  # close the file

        #shutil.rmtree(delete_id + '.' + delete_type)




def output():
    output = Output()
    #print(content)
    output.main()


    # result = open(paragraph_id +'.py', 'w')
    # code.extend(endcode)
    #
    # print(code)
    # # open for 'w'riting
    # for lines in code:
    #     result.write(lines)
    #     # 写入换行
    #     result.write('\n')
    #     # write text to file
    # result.close()  # close the file




if __name__ == '__main__':
    try:
        opts, argvs = getopt.getopt(sys.argv[1:], 's:lpaht', ['help'])
    except:
        print
        __doc__
        exit()

    for opt, argv in opts:
        if opt in ['-h', '--h', '--help']:
            print
            __doc__
            exit()
        elif opt == '-s':
            file_name = argv.split('.')[0]
            source_file = open(argv, 'r')
            content = source_file.read()
        elif opt == '-o':
            output()

