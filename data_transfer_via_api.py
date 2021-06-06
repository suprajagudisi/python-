from flask import Flask, request
import mysql.connector as connection
import pymongo
import csv
import os
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging as lg



app = Flask(__name__)


class mysql_homework:


    @app.route('/mysql_creation', methods=['post'])
    def create_mysql_database():
        """
        parmeters: user,passwd,name(create,insert,bulk_insertion,update,delete,download),host,database,tb_name,data
        """
        if not os.path.exists('os.getcwd() + "\\mysql.log" '):
            lg.basicConfig(filename=os.getcwd() + "\\mysql.log", filemode='w', level=lg.ERROR,format='%(asctime)s %(message)s')

        name =request.json['name']
        user = request.json['user']
        passwd = request.json['passwd']
        host = request.json['host']
        database = request.json['database']
        tablename = request.json['tb_name']
        try:
            mydb = connection.connect(host=host, user=str(user), database=database, passwd=passwd)
            lg.info("connection established")
        except Exception as e:
            lg.error("error has occured")
            lg.exception(str(e))
        if name == "create":
            data = request.json['data']
            try:

                query = "create table " + tablename + "("+data+")"
                cursor = mydb.cursor()
                cursor.execute(query)
                mydb.close()
                lg.info("table created")
                return "creation successful"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name =="insert":
            data = request.json['data']
            try:

                query = "insert into " +database+"."+ tablename + " values("+data+")"
                cursor = mydb.cursor()
                cursor.execute(query)
                mydb.commit()
                mydb.close()
                lg.info("insertion completed")
                return "insertion successful"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name =="update":

            try:
                data = request.json['data']
                condition =request.json['condition']

                query = "update " +database+"."+ tablename +" set " +data+" where "+condition
                cursor = mydb.cursor()
                cursor.execute(query)
                mydb.commit()
                mydb.close()
                lg.info("update successful")
                return "update successful"
            except Exception as e:
                mydb.close()
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name =="bulk_insertion":

            try:
                file = request.json['file']
                with open(file, 'r')as f:
                    d = csv.reader(f, delimiter='\n')

                    next(d)

                    for line in d:

                        query ="insert into {}.{} values({})".format(database,tablename,line[0])
                        cursor = mydb.cursor()
                        cursor.execute(query)
                        mydb.commit()
                mydb.close()
                lg.info("bulk data insertion successfull")
                return " bulk insertion successful"
            except Exception as e:
                mydb.close()
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name =="delete":
            try:
                condition =request.json['condition']

                query = "delete from " +database+"."+ tablename +" where " +condition+ ";"
                cursor = mydb.cursor()
                cursor.execute(query)
                mydb.commit()
                lg.info("record deleted")
                mydb.close()
                return " successfully deleted"
            except Exception as e:
                mydb.close()
                lg.error("error has occured")
                lg.exception(str(e))
                return e
        if name =="download":

            try:
                query = "select * from {}.{}".format(database,tablename)
                cursor = mydb.cursor()
                cursor.execute(query)

                with open("mydb_download.csv", "w", newline='') as csv_file:
                    write = csv.writer(csv_file)
                    write.writerow([i[0] for i in cursor.description])
                    write.writerows(cursor)
                    mydb.close()
                    lg.info("download completed")
                    return "download completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
class mongodb_homework:
    @app.route('/mongodb_creation',methods=['post'])
    def mongodb_creation():
        """
        parameters:url,name(create,insert,insert_many,update,delete,download),database,collection_name,data,type(one,many-for update and delete),file(bulk_insertion)

        """
        if not os.path.exists('os.getcwd() + "\\mongodb.log" '):
            lg.basicConfig(filename=os.getcwd() + "\\mongodb.log", filemode='w', level=lg.ERROR,format='%(asctime)s %(message)s')
        url = request.json['url']
        name = request.json['name']
        database = request.json['database']
        collection_name = request.json['collection_name']
        try:
            #url="mongodb://localhost:27017/"
            client = pymongo.MongoClient(url)
            lg.info("mongodb connection established")

        except Exception as e:
            lg.error("error has occured")
            lg.exception(str(e))
            print(e)

        if name =="create":
            try:


                #url = "mongodb://localhost:27017/"

                database2 = client[database]

                collection = database2[collection_name]
                lg.info("mongo database created")
                return "mongo database created"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "insert":
            try:

                data=request.json['data']
                database2 = client[database]

                col= database2[collection_name]
                col.insert_one(data)
                lg.info("mongo insertion completed")
                return "mongo insertion completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "insert_many":
            try:
                database2 = client[database]
                col = database2[collection_name]
                file=request.json['file']
                reader = csv.DictReader(open(file))

                col.insert_many(reader)
                lg.info("mongo bulk insertion completed")
                return "mongo bulk insertion completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "update":
            try:
                database2 = client[database]
                col = database2[collection_name]
                data=request.json['data']
                new_values= request.json['new_values']
                type=request.json['type']
                if type =="one":
                    col.update_one(data,new_values)
                    lg.info("mongo update completed")
                    return "mongo update completed"
                if type =="many":
                    col.update_many(data, new_values)
                    lg.info("mongo update completed")
                    return "mongo update completed"

            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "delete":
            try:
                database2 = client[database]
                col = database2[collection_name]
                type = request.json['type']
                data=request.json['data']

                if type == "many":
                    col.delete_many(data)
                    lg.info("mongo update completed")
                    return "mongo update completed"
                if type == "one":
                    col.delete_one(data)
                    lg.info("mongo update completed")
                    return "mongo update completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "download":
            try:


                database2 = client[database]

                col= database2[collection_name]
                columns= request.json['columns']
                array=col.find()
                a_list = columns.split(',')
                print(a_list)
                with open("m2_download.csv","w",newline="") as csvfile:
                    write =csv.DictWriter(csvfile,fieldnames= a_list)
                    write.writeheader()
                    for data in array:
                        write.writerow(data)



                lg.info("download completed")
                return "download completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
class cassandra_homework:
    @app.route('/cassandra_creation',methods=['post'])
    def cassandra_insertion_viapythonflask():
        """
        parameters:client_name,client_id,keyspace_name,table_name,data,name(create,insert,bulk_insertion,delete,update),columns
        """

        if not os.path.exists('os.getcwd() + "\\cassandra.log" '):
            lg.basicConfig(filename=os.getcwd() + "\\cassandra.log", filemode='w', level=lg.INFO,format='%(asctime)s %(message)s')
        try:

            cloud_config = {
                'secure_connect_bundle': "C:\cassandra\secure-connect-test.zip"
            }
            lg.info("cassandra connection established")
        except Exception as e:
            lg.error("error has occured")
            lg.exception(str(e))

        client_name= request.json['client_name']
        client_id = request.json['client_id']
        keyspace = request.json['keyspace_name']
        table_name = request.json['table_name']
        name = request.json['name']
        if name == "create":
            try:
                data =request.json['data']
                auth_provider = PlainTextAuthProvider(client_name,client_id)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()
                query = "CREATE TABLE "+keyspace+"."+table_name+ "("+data+")"
                row = session.execute(query).one()
                return "successfully inserted"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "insert":
            try:
                data =request.json['data']
                columns=request.json['columns']
                auth_provider = PlainTextAuthProvider(client_name,client_id)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()
                query = "insert into "+keyspace+"."+table_name+ " ("+columns+ ") values("+data+")"
                row = session.execute(query).one()
                lg.info("cassandra table created")
                return "successfully table created"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "bulk_insertion":
            try:

                columns=request.json['columns']
                file = request.json['file']
                auth_provider = PlainTextAuthProvider(client_name,client_id)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()

                with open(file, 'r')as f:
                    d = csv.reader(f, delimiter='\n')

                    next(d)

                    for line in d:
                        query = "insert into "+keyspace+"."+table_name+ " ("+columns+ ") values("+line[0]+")"
                        row = session.execute(query).one()
                f.close()
                lg.info("data inserted into cassandra")
                return "successfully inserted"
            except Exception as e:
                f.close()
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
        if name == "update":
            try:
                data =request.json['data']
                condition=request.json['condition']
                auth_provider = PlainTextAuthProvider(client_name,client_id)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()
                query = "update " +keyspace+"."+ table_name +" set " +data+" where "+condition
                row = session.execute(query).one()
                lg.info("cassandra updated")
                return "successfully updated"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)

        if name == "download":
            try:


                auth_provider = PlainTextAuthProvider(client_name,client_id)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()

                columns=request.json['columns']
                row = session.execute("""select * from {}.{}""".format(keyspace,table_name))
                df=pd.DataFrame(row)
                listed =columns.split(",")

                df.to_csv("c_download.csv",header=listed,index=False)
                lg.info("download completed")
                return "download completed"
            except Exception as e:
                lg.error("error has occured")
                lg.exception(str(e))
                return str(e)
if __name__ == '__main__':
    app.run()
