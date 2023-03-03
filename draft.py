'''
создать пользователя
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

запустить airflow на порту 8090
airflow webserver -p 8090

airflow scheduler
'''
import os
print(os.getcwd())
from pdb_helper import spark


# spark.sql(f"DESCRIBE HISTORY register_pdb_actualizer").show()

spark.sql(f"SELECT * FROM register_pdb_actualizer").show()
#spark.sql(f"Update register_pdb_actualizer set checksum = concat('a',checksum);").show()
#spark.sql(f"SELECT * FROM register_pdb_actualizer").show()

# spark.sql(f"RESTORE TABLE register_pdb_actualizer TO VERSION AS OF 2").show()
# spark.sql(f"SELECT * FROM register_pdb_actualizer version as of 2").show()


# spark.sql(f"create table blah(name string) USING csv;")
# spark.sql(f"insert into blah(name) select '32145';")
# spark.sql(f"truncate table blah;")

# spark.sql(f"drop table blah;")
# spark.sql(f"create table blah(name string) USING delta;")
# spark.sql(f"insert into blah(name) select '32145';")
# spark.sql(f"truncate table blah;")
# spark.sql(f"drop table blah;")


