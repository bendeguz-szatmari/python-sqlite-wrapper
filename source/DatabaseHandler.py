import sqlite3
from LogHandler import LogHandler


class DatabaseHandler:
    """ database handler class """

    def __init__(self, path, log_level=0):
        """
        :param path: path to the db, including db name
        :param log_level: 0-4, Debug...Critical, see LogHandler class
        """
        self.__path = path
        self.__tables = {}
        self.__keywords = {}
        self.__lh = LogHandler(log_level)

    def open(self):
        """ open database """
        self.__db = sqlite3.connect(self.__path)
        self.__dbcursor = self.__db.cursor()
        self.__lh.info("DB is opened: " + self.__path)

    def close(self):
        """ close database """
        self.__db.close()
        self.__lh.info("DB is closed: " + self.__path)

    def get_table_name(self, name):
        """
        get columns of the selected table
        :param name: tablen ame
        :return: list of columns
        """
        self.__dbcursor.execute("PRAGMA table_info('"+name+"')")
        fetch = self.__dbcursor.fetchall()
        columns = []
        for col in fetch:
            columns.append(col[1])
        self.__lh.debug("Table columns: " + ", ".join(columns))
        return columns

    def set_used_table(self, name):
        """
        set which table is used from database
        :param name: table name
        """
        self.__tables[name] = self.get_table_name(name)
        self.__lh.info("New table is set to used: " + name)

    def release_used_table(self, name):
        """
        release the given table
        :param name: table name
        """
        del self.__tables[name]
        self.__lh.info("Table is released from used tables: " + name)

    def clear_used_tables(self):
        """ release all used tables """
        self.__tables.clear()
        self.__lh.info("All tables are released from used tables")

    def execute_query(self, query):
        """
        executes the given sqlite query string
        :param query: sqlite query string
        :return: db's response to the query
        """
        try:
            self.__dbcursor.execute(query)
            self.__lh.info("Executed query: " + query)
        except Exception as e:
            self.__lh.exceptionHandling(e)
        return  self.__dbcursor.fetchall()

    def commit(self):
        """ apply changes done to database """
        self.__db.commit()

    def insert(self, table_name, data, columns=[]):
        """
        builds and executes INSERT query
        :param table_name: used table's name
        :param data: list of strings, data of a row to be inserted to the given table
        :param columns: list of columns where the given data should be inserted
        """
        if self.is_registered(table_name,data,columns) == True:
            self.__lh.warning("Record already inserted")
        else:
            try:
                keyword = "INSERT INTO"
                keyword2 = "VALUES"
                cols = "("+",".join(columns)+")"
                args = "("+",".join(data)+")"
                query = " ".join([keyword, table_name, cols, keyword2, args]) + ";"
                self.execute_query(query)
            except Exception as e:
                self.__lh.exceptionHandling(e)
            

    def select(self, table_name, cols=[], condition=[], distinct=False):
        """
        builds and executes SELECT query
        :param table_name: used table's name
        :param cols: list of columns should be selected
        :param condition: list of tuples, conditions of the query, use tupleListToStatement function to generate this string list
        :param distinct: set True if SELECT DISTICT query is required
        :return: list of tuple list, tuple list contains the selected data of one line
        """
        zipped=[]
        try:
            if cols == []:
                raise Exception("Empty selected columns list")

            keyword = "SELECT" if distinct == False else "SELECT DISTINCT"
            if cols == ["*"]:
                args = "*"
            else:
                args = ", ".join(cols)
            table = "FROM " + table_name
            if condition != []:
                where = self.where_clause(condition)
            else:
                where = ""
            query = " ".join([keyword, args, table, where]) + ";"
            rows = self.execute_query(query)
            for row in rows:
                self.__lh.debug("Select raw return values: " + (str(row)))
                zipped.append(list(zip(cols, row)))
                line = ""
                for index in range(0, len(cols)):
                    line = line + str(cols[index]) + " = " + str(row[index])
                    if index is not (len(cols)-1):
                        line += ", "
                self.__lh.debug("Returned line: " + line)
            self.__lh.debug(zipped)
        except Exception as e:
            self.__lh.exceptionHandling(e)
        finally:
            return zipped

    def update(self, table_name, data=[], condition=[]):
        """
        builds and executes UPDATE query
        :param table_name: used table's name
        :param data: list of tuple, see tupleListToStatement function
        :param condition: list of tuple, see whereClause
        """
        keyword = "UPDATE " + table_name + " SET"
        for i in range (0,len(data)):
            data[i] = (data[i][0], "=", data[i][1])
        d_args = self.tuple_list_to_statement(data)
        c_args = self.where_clause(condition)
        query = " ".join([keyword, d_args, c_args]) + ";"
        self.execute_query(query)

    def delete(self, table_name, condition=[]):
        """
        builds and executes DELETE query
        :param table_name: used table's name
        :param condition: list of tuple, see whereClause
        """
        keyword = "DELETE FROM " + table_name
        c_args = self.where_clause(condition)
        query = " ".join([keyword, c_args]) + ";"
        self.execute_query(query)

    def where_clause(self, condition=[]):
        """
        builds WHERE clause
        :param condition: list of tuple, see tupleListToStatement function
        :return: WHERE clause string
        """
        clause = "WHERE " + self.tuple_list_to_statement(condition)
        self.__lh.debug("WHERE clause: " + clause)
        return clause

    def and_clause(self):
        # TODO
        pass

    def or_clause(self):
        # TODO
        pass

    def like_clause(self):
        # TODO
        pass

    def glob_clause(self):
        # TODO
        pass

    def limit_clause(self):
        # TODO
        pass

    def order_by_clause(self):
        # TODO
        pass

    def group_by_clause(self):
        # TODO
        pass

    def having_clause(self):
        # TODO
        pass

    def tuple_list_to_statement(self, tuple_list=[]):
        """
        builds string from tuple
        :param tuple_list: tuple list, it should look like this when building conditions: [(column, relation operator, value), (c,ro,v),...]
        :return: list of strings
        """
        statement = ""
        for t in tuple_list:
            tmp = str(t[0]) + " " + str(t[1]) + " " + str(t[2])
            if statement == "":
                statement = tmp
            else:
                statement = ",".join([statement, tmp])
        return statement

    def is_registered(self, table_name, data_list=[], col_list=[]):
        ret = False
        new_list = []
        self.__lh.debug(data_list)
        self.__lh.debug(col_list)
        for x in range(len(data_list)):
            new_list.append( (data_list[x], "=", col_list[x]) )

        result =  []
        #result = self.select(table_name,"*",self.tuple_list_to_statement(new_list))
        if result != []:
            ret = True
        return ret
