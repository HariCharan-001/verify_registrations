import psycopg2
import sys
import os
import pandas, numpy

# config variables, remain constant throughout the execution
db_pwd = 'postgres'
base_dir = os.path.dirname(os.path.abspath(__file__))
response_files = []

if(len(sys.argv) > 1):
    db_pwd = sys.argv[1]

for file in os.listdir(base_dir + '/sports_responses'):
    if file.endswith(".csv"):
        response_files.append(base_dir + '/sports_responses/' + file)

def establish_connection():
    try:
        connection = psycopg2.connect(
            user = "postgres",
            password = db_pwd,
            host = "65.1.214.87",
            port = "5432",
            database = "csaiitm"
        )

        print(str(connection.get_dsn_parameters()) + "\n")

    except (Exception, psycopg2.Error) as error :
        print("Error while connecting to PostgreSQL : " + str(error))
        exit()

    return connection


def close_connection(connection):
    if(connection):
        connection.close()
        print("PostgreSQL connection is closed")

def get_members():
    global connection, cursor
    cursor.execute("SELECT * from members where payment_status = 'successful' and razorpay_payment_id IS NOT NULL")
    members = cursor.fetchall()
    return members



connection = establish_connection()
cursor = connection.cursor()
print('Connected to PostgreSQL' + '\n')

members = get_members()
names = members[1]
roll_nos = members[2]

for response_file in response_files:
    df = pandas.read_csv(response_file)
    print(response_file.split('/')[-1])
    print(' \n'.join(df.columns.tolist()) + '\n')
    
    # iterate through the columns that contain 'roll' or 'name' in their column name, ignore case
    for col in df.columns:
        if 'roll' in col.lower() or 'name' in col.lower():

            # iterate through rows of the column
            for index, row in df.iterrows():

                # if the entry is null, skip
                if pandas.isnull(row[col]):
                    continue

                values = row[col].split(' ')
    
