#!/usr/bin/env python
# -*- coding: utf-8 -*-



import re
import sys
import getopt
import os
import shutil
# import graph
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

endcode_scala = ['System.exit(0)']

blank = '   '

shell_command = []

assignment_statement = ['=','+','!','-']

pyspark_submit = '$SPARK_HOME/bin/spark-shell --executor-memory 5g --master local  '

scala_submit = '$SPARK_HOME/bin/spark-shell --executor-memory 5g --master local -i  '

python_submit = 'python '

endcode = ['if __name__ == "__main__":','   main()']

paragraph_id = 'default'

file_type ={'python': 'py','pyspark': 'py', 'sh': 'sh', 'spark': 'scala', 'angular':'js'}

interpreter = ['pyspark','sh','spark','angular','python',]

generate_type = []

generate_id = []

All_Variables = {}

All_Functions = {}

Function_Contents = {}

Var_Values = {}

pycode = []

initial_graph = []

dependency_text = []

# DependencyGraph = graph.Graph(initial_graph , directed=True)





class Output(object):

    line = ''
    paragraph_id = ''
    type_temp = ''
    variable = []
    function = []
    dependency = []
    code = []

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
        sh_file = open( 'Variables.txt', 'w')
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

    def export_sh(self):
        sh_file = open('command.sh', 'w')
        for commandlines in shell_command:
            sh_file.write(commandlines)
            sh_file.write('\n')
        sh_file.close()

    def export_dependency(self):
        dep_file = open('dependency.txt', 'w')
        for dependencylines in dependency_text:
            dep_file.write(dependencylines)
            dep_file.write('\n')
        dep_file.close()

    def main_process(self, textOfParagraph ):
        global dependency
        function_called = []
        tag = 9
        function_generate_code = []
        function_generate = []  # Save the name of function that are need to be generated in the script
        code = []
        variable = []
        function = []
        # dependency = []
        flag = 0
        line = ""
        # print textOfParagraph
        type_temp = ''
        while tag < len(textOfParagraph) and (textOfParagraph[tag].isalpha()):
            type_temp += textOfParagraph[tag]
            tag += 1
        # print "findTYPE"
        print (type_temp)
        # print type_temp
        # print (type_temp)
        if self.is_interpreter(type_temp):
            output_type = file_type[type_temp]
            delete_type = output_type
        # print(output_type)

        #Read Code
        while tag < len(textOfParagraph):

            # flag_n = 1;
            # if textOfParagraph[i] == '\\' :
            #    flag_n == -flag_n

            # print(i)
            if textOfParagraph[tag] == '\"':

                # print("End of Code")
                # print(i)
                if output_type == 'py':
                    line = blank + line

                code.append(line)
                # if output_type == 'sh':
                #     shell_command.append(line)

                line = ''
                # print(code)
                # print("___________________________END_______________________________")
                # flag = 1
                break

            elif (textOfParagraph[tag] == '\\') and textOfParagraph[tag + 1] == '\"':
                # print("Find\ & \"")


                tag += 1
                # print(i)
                # print(textOfParagraph[i])
                line += textOfParagraph[tag]
                tag += 1
                # print(line)
            elif textOfParagraph[tag] == '\\' and textOfParagraph[tag + 1] == 'n':
                if textOfParagraph[tag - 1] != '\\':
                    # print("Find \n")
                    line += ''

                    tag += 2
                    # print(i)
                    # print(textOfParagraph[i])
                    if output_type == 'py':
                        line = blank + line

                    code.append(line)

                    # if output_type == 'sh':
                    #     shell_command.append(line)

                    line = ''
                    # print(code)
                else:
                    tag += 2
                    line += 'n'
            elif textOfParagraph[tag] == '\\' and textOfParagraph[tag + 1] == 't':
                if textOfParagraph[tag - 1] != '\\':
                    # print("Find \n")
                    line += '   '
                    print line

                    tag += 2
                    # print(i)
                    # print(textOfParagraph[i])

                    # if output_type == 'sh':
                    #     shell_command.append(line)

                    # line = ''
                    # print(code)
                else:
                    tag += 2
                    line += 't'




            ###########dependency part##############
            #####find var#####
            elif output_type == 'py' and textOfParagraph[tag] == '=' and textOfParagraph[
                        tag - 1] not in assignment_statement and textOfParagraph[
                        tag + 1] not in assignment_statement:
                line += textOfParagraph[tag]

                j = 1
                k = 1
                while not textOfParagraph[tag - j].isalpha():
                    # print textOfParagraph[i - j]
                    # print line
                    j += 1
                while textOfParagraph[tag - j - k].isalpha() and not (
                                textOfParagraph[tag - j - k] == 'n' and textOfParagraph[tag - j - k - 1] == '\\'):
                    k += 1
                variable_temp = ''
                for x in range(1, k + 1):
                    variable_temp += textOfParagraph[tag - j - k + x]

                flag_find_dependency = 0;
                for key, value in All_Variables.items():
                    if variable_temp in value:
                        print('Variables in Current to Paragraph ' + key)
                        dependency.append(key)
                        flag_find_dependency = 1;
                if flag_find_dependency == 0:
                    variable.append(variable_temp)
                    Var_Values[variable_temp] = 0

                tag += 1

            #####find def######
            elif output_type == 'py' and textOfParagraph[tag] == 'd' and textOfParagraph[tag + 1] == 'e' and \
                            textOfParagraph[tag + 2] == 'f' and \
                            textOfParagraph[tag + 3] == ' ':
                print tag
                # print "————————————"
                # print textOfParagraph[tag]
                # print "————————————\n"
                tag += 4
                # print "————————————"
                # print textOfParagraph[tag]
                # print "————————————\n"
                line += 'def '
                function_defined = []
                tag = self.skip_blank(tag)
                def_func_temp = ''
                while textOfParagraph[tag].isalpha():
                    def_func_temp += textOfParagraph[tag]
                    tag += 1
                print "Def Func:"
                print def_func_temp
                function.append(def_func_temp)
                print "Def Func Temp:"
                print function
                line += def_func_temp

                ############
                while textOfParagraph[tag] != ':':
                    line += textOfParagraph[tag]
                    tag += 1

                tag += 1
                line += ':'
                function_defined.append(line)
                line = blank + line
                code.append(line)
                print code
                line = ''

                flag_end_of_func = 0
                while flag_end_of_func == 0:
                    if textOfParagraph[tag] == '\\' and textOfParagraph[tag + 1] == 'n':
                        if textOfParagraph[tag + 2].isalpha():
                            print line
                            flag_end_of_func = 1
                            print ("end of func")
                            # print flag_end_of_func
                        function_defined.append(line)
                        print "————————————"
                        print function_defined
                        print "————————————\n"

                        line = blank + line
                        tag += 2
                        # print line
                        code.append(line)
                        line = ''
                    else:
                        line += textOfParagraph[tag]
                        tag += 1
                Function_Contents[def_func_temp] = function_defined
                # print Function_Contents

            #####find func#####
            elif output_type == 'py' and textOfParagraph[tag] == '(':
                line += textOfParagraph[tag]
                j = 1
                k = 1
                while not textOfParagraph[tag - j].isalpha():
                    # print textOfParagraph[i - j]
                    # print line
                    j += 1
                while textOfParagraph[tag - j - k].isalpha() and not (
                                textOfParagraph[tag - j - k] == 'n' and textOfParagraph[tag - j - k - 1] == '\\'):
                    k += 1
                function_temp = ''
                for x in range(1, k + 1):
                    function_temp += textOfParagraph[tag - j - k + x]
                flag_find_dependency = 0;
                # print "————————————"
                # print function_temp
                # print All_Functions
                for key, value in All_Functions.items():
                    if function_temp in value and function_temp not in function_called:
                        print('Functions in Current to Paragraph ' + key)
                        dependency.append(key)
                        function_called.append(function_temp)
                        # Append Function Code to Output
                        function_code = Function_Contents[function_temp]
                        function_generate_code[len(function_generate_code):len(function_code)] = function_code

                        function_generate.append(function_temp)
                        flag_find_dependency = 1;

                tag += 1



            ######################################3##

            else:
                line += textOfParagraph[tag]
                tag += 1



                ###########dependency part##############

                ######################################3##



                # Merge Python Code

                # if output_type == 'py' and type_temp == 'python':
                #     pycode[len(pycode):len(code)] = code
                #     code = pycode[:]
                #     print(pycode)
        #Read ID
        while tag < len(textOfParagraph) and flag != 1:
            if (textOfParagraph[tag] == '\"') and (textOfParagraph[tag + 1] == 'i') and (
                textOfParagraph[tag + 2] == 'd') and (
                        textOfParagraph[tag + 3] == '\"') and (textOfParagraph[tag + 4] == ':'):
                tag += 6
                global paragraph_id
                paragraph_id = ''
                while tag < len(textOfParagraph):
                    # print(tag)
                    if textOfParagraph[tag] == '\"':
                        print("End of ID")

                        # print(tag)

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

                        print All_Variables
                        print All_Functions
                        # 去重
                        dependency = self.delete_duplicated_element_from_list(dependency)
                        for nodes in dependency:
                            line = paragraph_id + " " + nodes
                            dependency_text.append(line)
                            print "-------------"
                            print dependency_text
                            print "-------------"
                            # DependencyGraph.add(paragraph_id, nodes)

                        ################################



                        if output_type == 'py' and type_temp == 'pyspark':

                            command = pyspark_submit + paragraph_id + '.py'
                            shell_command.append(command)

                        elif output_type == 'py' and type_temp == 'python':
                            command = python_submit + paragraph_id + '.py'
                            shell_command.append(command)

                        elif output_type == 'scala':
                            print "__________FIND SCALA__________"
                            command = scala_submit + paragraph_id + '.scala'
                            shell_command.append(command)

                        elif output_type == 'sh':

                            command = "./" + paragraph_id + '.sh'
                            shell_command.append(command)
                            #     sh_file = open('command.sh', 'w+')
                            #     sh_file.write(command)
                            #     sh_file.write('\n')
                            #     sh_file.close()
                            #     print('写入sh' + paragraph_id)
                            # command = ''


                        # if output_type == 'sh':



                        flag = 1
                        break

                    if (textOfParagraph[tag] == '\\') and textOfParagraph[tag + 1] == '\"':
                        # print("Find\ & \"")

                        tag += 1
                        # print(tag)
                        # print(textOfParagraph[tag])
                        paragraph_id += textOfParagraph[tag]
                        tag += 1
                        # print(line)

                    else:
                        paragraph_id += textOfParagraph[tag]
                        tag += 1
            else:
                tag += 1

        result = open(paragraph_id + '.' + output_type, 'w')

        if output_type == 'py':

            # code = pycode
            if type_temp == 'pyspark':
                code[0:0] = begincode_pyspark
            elif type_temp == 'python':
                code[0:0] = begincode_python_2
                code[0:0] = function_generate_code
                code[0:0] = begincode_python_1

            code[len(code):len(endcode)] = endcode

        if output_type == 'scala':

            code[len(code):len(endcode_scala)] = endcode_scala


                # print (code)
            # print (pycode)
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

        print dependency
        print function








    # 主程序
    def main(self):
        i = 0
        global dependency
        dependency = []

        # code = []
        # command = ''
        # flag = 0
        # line = ''
        while i < len(content):
            # i = self.skip_blank(i)
            # print(i)
            # if flag == 1:
            #   break

            if (content[i] == '\"') and (content[i + 1] == 't') and (content[i + 2] == 'e') and (
                content[i + 3] == 'x') and (content[i + 4] == 't') and (content[i + 5] == '\"') and (
                content[i + 6] == ':'):
                # 判断出text关键字"
                print "Find Text"
                print i
                i += 1
                text = "\""
                while i < len(content) and not ((content[i] == '\"') and (content[i + 1] == 't') and (content[i + 2] == 'e') and (
                content[i + 3] == 'x') and (content[i + 4] == 't') and (content[i + 5] == '\"') and (
                content[i + 6] == ':')):
                    text += content[i]
                    i += 1
                # print text
                self.main_process(text)
            else:
                i += 1
            text = ""

        self.export_sh()
        self.export_var_func()
        self.export_dependency()


        print Function_Contents
        # print "-------------"
        # print dependency
        # print "-------------"
        # pprint.pprint(DependencyGraph._graph)







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
            output()