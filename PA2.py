import mysql.connector
from mysql.connector import errorcode
import sys, os

cnx = mysql.connector.connect(user='root', password ='root', 
                              unix_socket=('/Applications/MAMP/tmp/mysql/mysql.sock'))

DB_NAME = 'dealershipDB'

cursor = cnx.cursor()

#Creates the database, will print error if failed
def create_database(cursor, DB_NAME):
  try:
    cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
  except mysql.connector.Error as err:
    print ("Failed creating database: {}".format(err))
    exit(1)

#Creates the table for brands.
def create_table_brands(cursor):
  create_brands = "CREATE TABLE `brands` (" \
    " `name` varchar(64) NOT NULL," \
    " `country` varchar(64)," \
    " `parent_co` varchar(64)," \
    " `ceo` varchar(64)," \
    " PRIMARY KEY (`name`)" \
    ") ENGINE = InnoDB"
  
  try:
    print("Creating table brands: ")
    cursor.execute(create_brands)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Already exists.")
    else:
      print(err.msg)
  else:
    print("OK")

#Creates the table for cars. 
def create_table_cars(cursor):
  create_cars = "CREATE TABLE `cars` (" \
    " `car_id` varchar(64) NOT NULL," \
    " `brand` varchar(64)," \
    " `model` varchar(64)," \
    " `year` SMALLINT," \
    " PRIMARY KEY (`car_id`)," \
    " FOREIGN KEY (`brand`) REFERENCES brands(name)" \
    ") ENGINE = InnoDB"
  
  try:
    print("Creating table cars: ")
    cursor.execute(create_cars)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Already exists.")
    else:
      print(err.msg)
  else:
    print("OK")

#Creates the table for listings. 
def create_table_listings(cursor):
  create_listings = "CREATE TABLE `listings` (" \
    " `vin_nr` varchar(64) NOT NULL," \
    " `car_id` varchar(64)," \
    " `color` varchar(64)," \
    " `miles` INT," \
    " `price` INT," \
    " PRIMARY KEY (`vin_nr`)," \
    " FOREIGN KEY (`car_id`) REFERENCES cars(car_id)" \
    ") ENGINE = InnoDB"
  
  try:
    print("Creating table listings: ")
    cursor.execute(create_listings)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Already exists.")
    else:
      print(err.msg)
  else:
    print("OK")

#Inserts the data from the file brands.csv into the table brands. You will need to to change
#the LOAD DATA INFILE 'directory' to match the directory of your 'brands.csv'.
#furthermore if you have trouble with secure_file_priv you will need to disable this in your mysql config.
def insert_into_brands(cursor):
  insert_sql =  "LOAD DATA INFILE '/Users/lk8562peterdahlberg/Desktop/python_courses/1DV503/data/brands.csv' "\
                "INTO TABLE brands "\
                "FIELDS TERMINATED BY ';' "\
                "ENCLOSED BY '\"' "\
                "LINES TERMINATED BY '\n' "\
                "IGNORE 1 ROWS "\

  try:
    print("Inserting data into brands:")
    cursor.execute(insert_sql)
  except mysql.connector.Error as err:
    print(err.msg)
  else:
    cnx.commit()
    print("OK")

#Inserts the data from the file cars.csv into the table cars. You will need to to change
#the LOAD DATA INFILE 'directory' to match the directory of your 'cars.csv'.
#furthermore if you have trouble with secure_file_priv you will need to disable this in your mysql config.
def insert_into_cars(cursor):
  insert_sql =  "LOAD DATA INFILE '/Users/lk8562peterdahlberg/Desktop/python_courses/1DV503/data/cars.csv' "\
                "INTO TABLE cars "\
                "FIELDS TERMINATED BY ';' "\
                "ENCLOSED BY '\"' "\
                "LINES TERMINATED BY '\n' "\
                "IGNORE 1 ROWS "\

  try:
    print("Inserting data into cars:")
    cursor.execute(insert_sql)
  except mysql.connector.Error as err:
    print(err.msg)
  else:
    cnx.commit()
    print("OK")

#Inserts the data from listings.csv into the table listings. You will need to to change
#the LOAD DATA INFILE 'directory' to match the directory of your 'listings.csv'.
#furthermore if you have trouble with secure_file_priv you will need to disable this in your mysql config.
def insert_into_listings(cursor):
  insert_sql =  "LOAD DATA INFILE '/Users/lk8562peterdahlberg/Desktop/python_courses/1DV503/data/listings.csv' "\
                "INTO TABLE listings "\
                "FIELDS TERMINATED BY ';' "\
                "ENCLOSED BY '\"' "\
                "LINES TERMINATED BY '\n' "\
                "IGNORE 1 ROWS "\

  try:
    print("Inserting data into listings:")
    cursor.execute(insert_sql)
  except mysql.connector.Error as err:
    print(err.msg)
  else:
    cnx.commit()
    print("OK")

try:
  cursor.execute("USE {}".format(DB_NAME)) #USE dealershipDB
except mysql.connector.Error as err:
  print("Database {} does not exist.".format(DB_NAME))
  if err.errno == errorcode.ER_BAD_DB_ERROR:
    create_database(cursor, DB_NAME)
    print("Database {} successfully created.".format(DB_NAME))
    cnx.database = DB_NAME
    create_table_brands(cursor)
    create_table_cars(cursor)
    create_table_listings(cursor)
    insert_into_brands(cursor)
    insert_into_cars(cursor)
    insert_into_listings(cursor)
  else:
    print(err)

#Below is the code for all of the queries depending on the option chosen from the main menu.
def query_1(cursor):
  query1_1 = "CREATE VIEW listings_info AS SELECT"\
           " cars.brand, cars.model, cars.year, listings.color, listings.miles, listings.price"\
           " FROM listings INNER JOIN cars ON listings.car_id=cars.car_id"\

  query1_2 = "SELECT * FROM listings_info"
  
  #Creates the view if not already created
  try:
    cursor.execute(query1_1)
  except Exception:
    pass

  cursor.execute(query1_2)

  results = cursor.fetchall()
  for row in results:
    print(row[0], row[1], row[2], ",", row[3], ",", row[4], "miles,", row[5], "$")


def query_2(cursor, answer_color):
  query2 = "SELECT cars.brand, cars.model, cars.year FROM cars, listings"\
           " WHERE listings.car_id=cars.car_id AND listings.color=%s"
  cursor.execute(query2, (answer_color,))
  results = cursor.fetchall()
  if results:
    for row in results:
      print(row[0], row[1], row[2])
  else:
    print("No vehicle has that color.")

def query_3(cursor):
  query3 = "SELECT AVG(price) FROM listings"
  cursor.execute(query3)
  results = cursor.fetchall()
  for row in results:
    print("The average price of a car is:", row[0], "$")
  
def query_4(cursor, answer_country):
  query4 = "SELECT cars.brand, cars.model, cars.year FROM brands, cars, listings"\
           " WHERE listings.car_id=cars.car_id AND cars.brand=brands.name"\
           " AND brands.country =%s"
  cursor.execute(query4, (answer_country,))
  results = cursor.fetchall()
  if results:
    for row in results:
      print(row[0], row[1], row[2])
  else:
    print("No cars from that country.")

def query_5(cursor):
  query5 = "SELECT cars.brand, COUNT(listings.vin_nr) FROM listings, cars"\
           " WHERE listings.car_id=cars.car_id GROUP BY cars.brand"
  cursor.execute(query5)
  results = cursor.fetchall()
  for row in results:
    print(row[0], ":", row[1])


#Main menu
def main_menu():
  print("------------------------")
  print("1. Show full info on all listings.")
  print("2. Search for what cars are available in a certain color.")
  print("3. Show average price of a car being sold.")
  print("4. Search for what cars are available from a certain brand country.")
  print("5. Search for how many cars are available from each brand.")
  print("Q. Quit.")
  print("------------------------")
  print("Please choose an option:")
  answer = str(input())
  return answer

#Function that waits for a user to press a key, this is cross platform,
#though I have only tested it with macOS as that's what I'm using.
#This can also be achieved with curses (though I don't know if it's cross platform).
def wait():
  key_press = None
  if os.name == 'nt':
    import msvcrt
    key_press = msvcrt.getch()
  else:
    import termios
    fd = sys.stdin.fileno()

    oldterm = termios.tcgetattr(fd)
    newattr = termios.tcgetattr(fd)
    newattr[3] = newattr[3] & ~termios.ICANON & ~termios.ECHO
    termios.tcsetattr(fd, termios.TCSANOW, newattr)

    try:
      key_press = sys.stdin.read(1)
    except IOError:
      pass
    finally:
      termios.tcsetattr(fd, termios.TCSAFLUSH, oldterm)
  return key_press



#All the different options, choosing quit will simply stop the while loop.
def answer_1():
  query_1(cursor)
  print("Press any key to return to main menu:")
  wait()

def answer_2():
  print("Enter the color:")
  answer_color = input()
  query_2(cursor, answer_color)
  print("Press any key to return to main menu:")
  wait()

def answer_3():
  query_3(cursor)
  print("Press any key to return to main menu:")
  wait()

def answer_4():
  print("Enter the country:")
  answer_country = input()
  query_4(cursor, answer_country)
  print("Press any key to return to main menu:")
  wait()

def answer_5():
  query_5(cursor)
  print("Press any key to return to main menu:")
  wait()

while True:
  answer = main_menu()

  if answer == "1":
    answer_1()

  elif answer == "2":
    answer_2()

  elif answer == "3":
    answer_3()

  elif answer == "4":
    answer_4()

  elif answer == "5":
    answer_5()

  elif answer == "Q" or answer == "q":
    print("Quitting.")
    break