#!/usr/bin/python

import csv
import sys
import psycopg2
import re
import time
import datetime
import traceback

# output_file = open('output_file', 'w')

# dbconnection = psycopg2.connect("dbname='kmart_totara_201608090050' user='kmart_totara' host='localhost' password='1q2w3e4r'")
# cursor = dbconnection.cursor()

# csvdata = csv.reader(open(file), delimiter=',')
# csvdata.next()

# existing_users = open('existing_users')

db_connection_string = "dbname='kmart_totara_201608090050' user='kmart_totara' host='localhost' password='1q2w3e4r'"

def get_course_shortname(file):
    with open(file, 'r') as f:
        course_name = f.readline().strip().split(',')[4]

    dbconnection = psycopg2.connect(db_connection_string)
    cursor = dbconnection.cursor()
    cursor.execute('''SELECT shortname FROM mdl_course where fullname = %s''', (course_name,))
    query_result = cursor.fetchone()
    cursor.close()
    dbconnection.close()

def get_user_id(user):
    dbconnection = psycopg2.connect(db_connection_string)
    cursor = dbconnection.cursor()
    cursor.execute('''SELECT id FROM mdl_user where username = %s''', (user,))
    query_result = cursor.fetchone()
    cursor.close()
    dbconnection.close()
    
    if query_result is not None:
        if query_result[0] is not None:
            return str(query_result[0])
    else:
        return False


def get_users_completion_from_file(user, file):
    user = user.rstrip()
    csvdata = csv.reader(open(file), delimiter=',')
    for line in csvdata:
        # print("Looking up ", user, " in line ", line)
        
        if user == line[2].strip():
            #found
            #print("Found in line ", line)
            return line #this is already an array ID,Name,Username,"Email address","Contractor Induction","Contractor Induction - Completion date","Course complete"
        else:
            continue

def save_moodle_formatted_completion_record(user, file):
    source_completion_record = get_users_completion_from_file(user, completions_file)
    
    if source_completion_record and source_completion_record[4] == 'Completed':
        print source_completion_record

        #conversion to unixtimestamp. considering all dates are in localtimezone.
        # date1 = time.mktime(datetime.datetime.strptime(source_completion_record[5], "%d/%m/%y, %H:%M").timetuple())
        # date2 = time.mktime(datetime.datetime.strptime(source_completion_record[6], "%d/%m/%y, %H:%M").timetuple())
        # date_course_completed = int(max(date1, date2))
        # print(date_course_completed)
        
        #now save this completion to another file in format of username,courseshortname,courseidnumber,completiondate,grade
        formatted_completion_record = '''USERNAME,Contractor,108,COMPLETIONDATE,'''
        formatted_completion_record = re.sub('USERNAME', user, formatted_completion_record)
        print formatted_completion_record
        formatted_completion_record = re.sub('COMPLETIONDATE', source_completion_record[5], formatted_completion_record)
        #finally write the completion record to file in Moodle-format
        file.write(formatted_completion_record + '\n')

def get_user_completion_from_db(user, course):
    dbconnection = psycopg2.connect(db_connection_string)
    cursor = dbconnection.cursor()
    cursor.execute('''SELECT timecompleted from mdl_course_completions where userid = %s and course = %s''', (get_user_id(user), course))
    
    query_result = cursor.fetchone()
    #print "user:", user, "query_result:", query_result[0]
    cursor.close()
    dbconnection.close()
    
    if query_result is not None and query_result[0] is not None:
        return str(query_result[0])
    else:
        return 0

def get_user_completion_history(user, course):
    dbconnection = psycopg2.connect(db_connection_string)
    cursor = dbconnection.cursor()
    cursor.execute('''SELECT timecompleted from mdl_course_completion_history where userid = %s and course = %s''', (get_user_id(user), 108))
    query_result = cursor.fetchone()
    cursor.close()
    dbconnection.close()
    
    if query_result is not None and query_result[0] is not None:
            return str(query_result[0])
    else:
        return 0

# For each user in Nick's completions CSV:
# Check if user exists in Totara and add him into the output course_completions.csv in Totara format:
# username,courseshortname,courseidnumber,completiondate,grade

with open(sys.argv[2], 'w') as output_file, open(sys.argv[1]) as input_completions_file:
    output_csv = csv.writer(output_file, delimiter=',')
    # write the first header
    output_csv.writerow(['username', 'courseshortname', 'courseidnumber', 'completiondate', 'grade'])

    # a trick to skip processing the first row in CSV
    iter_completions = iter(csv.reader(input_completions_file, delimiter=','))
    next(iter_completions)
    
    for record in iter_completions:
        print record
        try:
            if record[4].strip() == 'Completed':
                status = record[4].strip()
                print "Status:", status
                # check that user exists in Totara
                if not get_user_id(record[2].strip()):
                    print "===User is missing in DB==="
                else:
                    username = record[2].strip()
                    print "username:", username
                    new_timecompleted_day = record[5].split(',')[0] # taking only the day part of "29/12/15, 10:17"
                    new_timecompleted_unixtime = int(time.mktime(datetime.datetime.strptime(record[5], "%d/%m/%y, %H:%M").timetuple()))
                    print "new_timecompleted_unixtime:", new_timecompleted_unixtime
                    # Finally write record to output CSV in Totara format if date is greater than currently in DB:
                    timecompleted_db = get_user_completion_from_db(username, 108)
                    print "timecompleted_db:", timecompleted_db
                    if int(new_timecompleted_unixtime) > int(timecompleted_db):
                        print "===Updating the user==="
                        output_csv.writerow([username, 'Contractor', 'Contractor', new_timecompleted_day, 100])
        except Exception as err:
                print "Error at record:", record
                print traceback.format_exc(err)

# for user in open(existing_users):
#     user = user.strip()
#     print "+Checking user:", user, "id:", get_user_id(user)
#
#     #get completion record from DB
#     print "--Completion time in Moodle DB:", get_user_completion_from_db(user, 108)
#     #get completion record from Nicks file:
#     print "--Completion time in Nick's file:", get_users_completion_from_file(user, completions_file)[5]
#
#     #save_moodle_formatted_completion_record(user, output_file)
    
# for rec in csvdata:
#     user = rec[2]
#     print("=====================Checking user: ", user)
#     cursor.execute("""SELECT username FROM mdl_user WHERE username = %s;""", (user,))
#     try:
#         user_in_db = str(cursor.fetchone()[0])
#         #print("User in db: ", user_in_db)
#         if (user == user_in_db):
#             print("OK. user: ", user, " exists")
#     except:
#         print("BAD. user: ", user, " is missing")
#
# cursor.close()
# dbconnection.close()
