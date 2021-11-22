import pyodbc
import logging
import sys
import config

def send_notification():
    """Sending a notification to smartphone via Pushover app if an exception occurs."""
    from pushover import Client
    user_id = config.pushover['user_key']
    api_token = config.pushover['api_token']
    client = Client(user_id, api_token = api_token)
    client.send_message("Job na statistku otazek ve VU nedobehl.", title="VU statistika otazek")
    logging.info('Notifikace odeslána.')

# Crendetials for connection to database
server = config.vudb_prod["host"]
database = config.vudb_prod["database"]
username = config.vudb_prod["username"]
password = config.vudb_prod["password"]
log_path = config.log_path

# Logging settings
logging.basicConfig(filename = log_path, level = logging.INFO, format = '%(asctime)s:%(levelname)s:%(message)s')

# Connection to the database
try:
    db_con = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+\
                                    ';DATABASE='+database+';UID='+username+';PWD='+ password)
    
except Exception as e:
    logging.info(e)
    logging.info("Chyba pri pripojeni do DB")
    logging.info("Program ukoncen")
    send_notification()
    sys.exit()

cursor = db_con.cursor()

# Truncate table before inserting new data
sql = """TRUNCATE TABLE o2_qstats_jd"""
try:
    cursor.execute(sql)
    logging.info("Obsah o2_qstats_jd vymazan")
except Exception as e:
    logging.info(e)
    logging.info("Nepodarilo se vymazat o2_qstats_jd")
    logging.info("Program ukoncen")
    db_con.close()
    send_notification()
    sys.exit()

# SQL query for courses with keyword contains 'liveboard'
sql = """
select 
course_id as 'id_kurzu', 
name as 'nazev', 
language_id as 'jazyk',
k.key_words as 'key_words',
description as 'popis', 
create_date as 'datum_zalozeni', 
last_update_date as 'datum_zmeny'
from courses c
left join(
select fk_tutor_course, max(key_words) as "key_words"
from k_courses
where fk_tutor_course is not null
group by fk_tutor_course
) k on c.course_id = k.fk_tutor_course
where key_words LIKE '%liveboard%'
"""

# Execute the query and get data  
try:
    cursor.execute(sql)
    courses = cursor.fetchall()
    logging.info("Kurzy pro statistiku nacteny")
except Exception as e:
    logging.info(e)
    logging.info("Kurzy pro statistiku nenacteny")
    logging.info("Program ukoncen")
    db_con.close()
    send_notification()
    sys.exit()

pocet_kurzu = 0
pocet_otazek = 0

# For every course id it is needed to run SQL procedure, get data and insert them to a table
for course in courses:
    sql = f"""select * from dbo.jm_GetQStatInCourse({course[0]})
order by uspesnost desc, pocet desc"""
    try:
        cursor.execute(sql)
        stats = cursor.fetchall()
    except Exception as e:
        logging.info(e)
        logging.info(f"U kurzu id {course} se nepodarilo ziskat statistiku otazek")
        continue

    for row in stats:
        #print(row[0], row[1], row[2], row[3], row[4], row[5])
        sql = f"""INSERT INTO o2_qstats_jd("id_kurzu","id otazky","text otazky",pocet,spravne,spatne,uspesnost)
        VALUES({course[0]}, {row[0]}, '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]})
"""
        # print(sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            logging.info(e)
            logging.info(f"Kurz: {course}, otazka: {row[0]} - insert se nezdaril")
            continue
        pocet_otazek +=1
    
    pocet_kurzu += 1

# Append to logging file
# Number of courses
logging.info(f"Pocet dohledavanych kurzu: {pocet_kurzu}")
# Number of questions
logging.info(f"Pocet vlozenych otazek: {pocet_otazek}")
logging.info(f"Program ukoncen bez chyby")
db_con.commit()
db_con.close()
