# creating and manipulating database with Python

#                https://dev.mysql.com/downloads/mysql/       # from this website it is possible to download MySQL

# Check if Python is installed
# if Python is installed:

# move to windows command prompt
# let's move to the folder where python is installed

#             pip help         # to check if PIP is installed

# if PIP does not respond, download it typing:

#             curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py        # to download the file python get-pip.py

#             python get-pip.py      # to install PIP in Python

#             python -m pip install -U pip

#             pip help         # again to check if PIP is installed

#             pip install mysql-connector-python      # install library Python MySQL Connector

#             pip install pandas        # install library pandas



##### Let's start with code in Python  #####

# let's import libraries we use:
import mysql.connector
from mysql.connector import Error
import pandas as pd

# define a function to create the connection:
def create_server_connection(host_name, user_name, user_password):
    connection = None   # close every connection was open
    # to manage error we use try/except
    try:
        # let's connect with MySQL:
        connection = mysql.connector.connect(
        host=host_name,
        user=user_name,
        passwd=user_password
        )
        print("MySQL Database connection successful")
        # in case of failure:
    except Error as err:
        print(f"Error: '{err}'")
    return connection   # return object connection
    
# let's call the function getting the object:
connection = create_server_connection("localhost", "root", "your MySQL password")   # password as string

# let's type a function to create a database, to run a query inside MySQL:
def create_database(connection, query):
    cursor = connection.cursor()  # method cursor on object connection (connection is the server)
    try:
        cursor.execute(query)  #method execute on object cursor (cursor is connection.cursor)
                               #query is what we want to ask to MySQL
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        
create_database_query = "CREATE DATABASE school"   #assign the query to an object
create_database(connection, create_database_query)  #call the function asking what to do, creating a database school

#let's change the function with a new argument to pass, database_name because it is possible to have more db in MySQL:
def create_db_connection(host_name, user_name, user_password, db_name):   # new argument db_name
    connection = None
    try:
        connection = mysql.connector.connect(
        host=host_name,
        user=user_name,
        passwd=user_password,
        database=db_name   # new instruction
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

#let's create a function to run a query:
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)  # that's the instruction to execute the query
        connection.commit()   # new instruction
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
        
#let's create a string in Python with the MySQL query(CREATE TABLE teacher):
create_teacher_table = """
CREATE TABLE teacher (
  teacher_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  language_1 VARCHAR(3) NOT NULL,
  language_2 VARCHAR(3),
  dob DATE,
  tax_id INT UNIQUE,
  phone_no VARCHAR(20)
  );
 """

connection = create_db_connection("localhost", "root", "your MySQL password", "your database") # connect to the database
execute_query(connection, create_teacher_table) # let's call the function with the query defined

#we do the same with client:
create_client_table = """
CREATE TABLE client (
  client_id INT PRIMARY KEY,
  client_name VARCHAR(40) NOT NULL,
  address VARCHAR(60) NOT NULL,
  industry VARCHAR(20)
);
 """

# we do the same with participant:
create_participant_table = """
CREATE TABLE participant (
  participant_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  phone_no VARCHAR(20),
  client INT
);
"""

# the same with course:
create_course_table = """
CREATE TABLE course (
  course_id INT PRIMARY KEY,
  course_name VARCHAR(40) NOT NULL,
  language VARCHAR(3) NOT NULL,
  level VARCHAR(2),
  course_length_weeks INT,
  start_date DATE,
  in_school BOOLEAN,
  teacher INT,
  client INT
);
"""

# connection = create_db_connection("localhost", "root", pw, db)
#let's call all funciotns:
execute_query(connection, create_client_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_course_table)

# now we have to define relations between these tables creating one or more
# tables to manage relations mant-to-many between tables partecipant and course:
alter_participant = """
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

alter_course = """
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES teacher(teacher_id)
ON DELETE SET NULL;
"""

alter_course_again = """
ALTER TABLE course
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

#let's create an other table:
create_takescourse_table = """
CREATE TABLE takes_course (
  participant_id INT,
  course_id INT,
  PRIMARY KEY(participant_id, course_id),
  FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE,
  FOREIGN KEY(course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
"""

# connection = create_db_connection("localhost", "root", pw, db)
#let's call functions:
execute_query(connection, alter_participant)
execute_query(connection, alter_course)
execute_query(connection, alter_course_again)
execute_query(connection, create_takescourse_table)
# now tables are created with appropriate obligations: primary keys and relation with outside keys

#let's add data to the tables, using the query(INSERT INTO tablename VALUES):
pop_teacher = """
INSERT INTO teacher VALUES
(1,  'James', 'Smith', 'ENG', NULL, '1985-04-20', 12345, '+491774553676'),
(2, 'Stefanie',  'Martin',  'FRA', NULL,  '1970-02-17', 23456, '+491234567890'), 
(3, 'Steve', 'Wang',  'MAN', 'ENG', '1990-11-12', 34567, '+447840921333'),
(4, 'Friederike',  'Müller-Rossi', 'DEU', 'ITA', '1987-07-07',  45678, '+492345678901'),
(5, 'Isobel', 'Ivanova', 'RUS', 'ENG', '1963-05-30',  56789, '+491772635467'),
(6, 'Niamh', 'Murphy', 'ENG', 'IRI', '1995-09-08',  67890, '+491231231232');
"""

# connection = create_db_connection("localhost", "root", pw, db)
# let's call the function:
execute_query(connection, pop_teacher)

#we do the same with other tables:
pop_client = """
INSERT INTO client VALUES
(101, 'Big Business Federation', '123 Falschungstraße, 10999 Berlin', 'NGO'),
(102, 'eCommerce GmbH', '27 Ersatz Allee, 10317 Berlin', 'Retail'),
(103, 'AutoMaker AG',  '20 Künstlichstraße, 10023 Berlin', 'Auto'),
(104, 'Banko Bank',  '12 Betrugstraße, 12345 Berlin', 'Banking'),
(105, 'WeMoveIt GmbH', '138 Arglistweg, 10065 Berlin', 'Logistics');
"""

pop_participant = """
INSERT INTO participant VALUES
(101, 'Marina', 'Berg','491635558182', 101),
(102, 'Andrea', 'Duerr', '49159555740', 101),
(103, 'Philipp', 'Probst',  '49155555692', 102),
(104, 'René',  'Brandt',  '4916355546',  102),
(105, 'Susanne', 'Shuster', '49155555779', 102),
(106, 'Christian', 'Schreiner', '49162555375', 101),
(107, 'Harry', 'Kim', '49177555633', 101),
(108, 'Jan', 'Nowak', '49151555824', 101),
(109, 'Pablo', 'Garcia',  '49162555176', 101),
(110, 'Melanie', 'Dreschler', '49151555527', 103),
(111, 'Dieter', 'Durr',  '49178555311', 103),
(112, 'Max', 'Mustermann', '49152555195', 104),
(113, 'Maxine', 'Mustermann', '49177555355', 104),
(114, 'Heiko', 'Fleischer', '49155555581', 105);
"""

pop_course = """
INSERT INTO course VALUES
(12, 'English for Logistics', 'ENG', 'A1', 10, '2020-02-01', TRUE,  1, 105),
(13, 'Beginner English', 'ENG', 'A2', 40, '2019-11-12',  FALSE, 6, 101),
(14, 'Intermediate English', 'ENG', 'B2', 40, '2019-11-12', FALSE, 6, 101),
(15, 'Advanced English', 'ENG', 'C1', 40, '2019-11-12', FALSE, 6, 101),
(16, 'Mandarin für Autoindustrie', 'MAN', 'B1', 15, '2020-01-15', TRUE, 3, 103),
(17, 'Français intermédiaire', 'FRA', 'B1',  18, '2020-04-03', FALSE, 2, 101),
(18, 'Deutsch für Anfänger', 'DEU', 'A2', 8, '2020-02-14', TRUE, 4, 102),
(19, 'Intermediate English', 'ENG', 'B2', 10, '2020-03-29', FALSE, 1, 104),
(20, 'Fortgeschrittenes Russisch', 'RUS', 'C1',  4, '2020-04-08',  FALSE, 5, 103);
"""

pop_takescourse = """
INSERT INTO takes_course VALUES
(101, 15),
(101, 17),
(102, 17),
(103, 18),
(104, 18),
(105, 18),
(106, 13),
(107, 13),
(108, 13),
(109, 14),
(109, 15),
(110, 16),
(110, 20),
(111, 16),
(114, 12),
(112, 19),
(113, 19);
"""

# connection = create_db_connection("localhost", "root", pw, db)
# let's call the functions:
execute_query(connection, pop_client)
execute_query(connection, pop_participant)
execute_query(connection, pop_course)
execute_query(connection, pop_takescourse)

#let's read data without any change in MySQL:
def read_query(connection, query):
    cursor = connection.cursor()
    result = None    # we reset th object result
    try:
        cursor.execute(query)
        result = cursor.fetchall()    # to read data without any change in the db
        return result
    except Error as err:
        print(f"Error: '{err}'")
       
#let's test the result:
#create a query, argument
q1 = """
SELECT *
FROM teacher;
"""

# connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)    # call the function

for result in results:
  print(result)   # to show all rows

#let's join tables:
#query:
q5 = """
SELECT course.course_id, course.course_name, course.language, client.client_name, client.address  
FROM course
JOIN client    
ON course.client = client.client_id    
WHERE course.in_school = FALSE;   
"""
# select these columns from two tables     # join client to course   # common values in columns for course and client    # condition

#connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q5) # call the function

for result in results:
  print(result)   # print results

#formatting output in a list:
#Inizializza una lista vuota
from_db = []

# Itera sui risultati e aggiungili alla fine della lista

# Restituisci una lista di tuple
for result in results:
  result = result
  from_db.append(result)
  print(from_db)

# Restituisci una lista di liste
from_db = []

for result in results:
  result = list(result)
  from_db.append(result)
  print(from_db)

# Restituisci una lista di lista e poi crea un dataframe pandas
from_db = []

for result in results:
  result = list(result)
  from_db.append(result)


columns = ["course_id", "course_name", "language", "client_name", "address"]
df = pd.DataFrame(from_db, columns=columns)

display(df)  # or
print(df)   # or
df

#update data:
#query, argument using intruction UPDATE:
update = """
UPDATE client 
SET address = '23 Fingiertweg, 14534 Berlin' 
WHERE client_id = 101;
"""

# connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, update)

#verify
q1 = """
SELECT *
FROM course;
"""

#connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)

#let's remove data:
delete_course = """
DELETE FROM course 
WHERE course_id = 20;
"""

# connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, delete_course)

#verify:
q1 = """
SELECT *
FROM course;
"""

#connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)

#let's create data from a list:
#define a function:
def execute_list_query(connection, sql, val):   # parameters sql, val
    cursor = connection.cursor()   # create object connection
    try:
        cursor.executemany(sql, val)   # new instruction
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#let's create the query argument:
sql = '''
    INSERT INTO teacher (teacher_id, first_name, last_name, language_1, language_2, dob, tax_id, phone_no) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    # %s is a segnaposto for the valuese in val object. Thanks to executemany method we can do this work with more values

#let's create a list with values:
val = [
    (7, 'Hank', 'Dodson', 'ENG', None, '1991-12-23', 11111, '+491772345678'), 
    (8, 'Sue', 'Perkins', 'MAN', 'ENG', '1976-02-02', 22222, '+491443456432')
]

# connection = create_db_connection("localhost", "root", pw, db)
#let's call the function:
execute_list_query(connection, sql, val)

#verify
q1 = """
SELECT *
FROM teacher;
"""

#connection = create_db_connection("localhost", "root", pw, db)
results = read_query(connection, q1)

from_db = []

for result in results:
  print(result)

