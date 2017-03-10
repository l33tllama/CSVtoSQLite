import csv, sqlite3, json

#TODO: move to config or something

config_filename = "config.json"

def create_table(c, table_name, column_names):

    # delete the table if it exists to flush the data when running again
    c.execute('DROP TABLE IF EXISTS ' + table_name)

    create_table_cmd = 'CREATE TABLE IF NOT EXISTS ' + table_name + '('

    for column in column_names:
        name, type = column[0], column[1]
        create_table_cmd += name + " " + type + ", "

    create_table_cmd = create_table_cmd[:-2]
    create_table_cmd += ");"
    print (create_table_cmd)
    c.execute(create_table_cmd)


def convert_csv(csv_filename, table_name, column_names, delimiter, sqlite3_db):

    conn = sqlite3.connect(sqlite3_db)
    c = conn.cursor()
    create_table(c, table_name, column_names)

    with open(csv_filename, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quotechar="|")
        for row in reader:
            insert_cmd = "INSERT INTO " + table_name + " VALUES("
            column_id = 0

            # print "row len: " + str(len(row)) + " " + str(len(column_names))

            for column in column_names:
                try:
                    value = row[column_id]
                except IndexError as e:
                    for col in row:
                        print col
                    print e

                if len(value) is 0:
                    print "No val for " + str(column[0])
                    value ='""'
                insert_cmd += value + ", "
                column_id += 1
            insert_cmd = insert_cmd[:-2]
            insert_cmd += ");"
            #print(insert_cmd)
            c.execute(insert_cmd)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    data = []

    with open(config_filename) as data_file:
        data = json.load(data_file)

    csv_filename = data['csv_filename']
    table_name = data['table_name']
    column_names = data['column_names']
    sqlite3_db = data['sqlite3_db']
    delimiter = str(data['delimiter'])

    convert_csv(csv_filename, table_name, column_names, delimiter, sqlite3_db)