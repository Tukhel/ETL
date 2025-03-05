/*
chcp 65001 && spark-shell -i D:\Education\ETL\ETL_new\Seminars\HW_03\s03_HW.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("D:/Education/ETL/ETL_new/Seminars/HW_03/s3.xlsx")
		df1.show()
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_03_task01")
        .mode("overwrite").save()
val q = """SELECT ID_Тикета, FROM_UNIXTIME(Status_Time) Status_Time,
(LEAD(Status_Time) OVER(PARTITION BY ID_Тикета ORDER BY Status_Time) - Status_Time)/3600 Длительность, 
case when Статус IS NULL then @PREV1
ELSE @PREV1:=Статус END 
Статус, 
case when Группа IS NULL then @PREV2
ELSE @PREV2:=Группа END 
Группа, Назначение FROM 
(SELECT ID_Тикета, Status_Time, Статус, if (ROW_NUMBER() OVER(PARTITION BY ID_Тикета ORDER BY Status_Time) = 1 AND Назначение IS NULL, '', Группа) Группа, Назначение FROM
(SELECT DISTINCT a.objectid ID_Тикета, a.restime Status_Time, Статус, Группа, Назначение, 
(SELECT @PREV1:=''),(SELECT @PREV2:='') FROM (SELECT DISTINCT objectid, restime FROM spark.sem_03_task01
WHERE fieldname IN ('GNAME2', 'Status')) a
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM spark.sem_03_task01 
WHERE fieldname IN ('Status')) a1
ON a.objectid = a1.objectid AND a.restime = a1.restime
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа, 1 Назначение FROM spark.sem_03_task01
WHERE fieldname IN ('GNAME2')) a2
ON a.objectid = a2.objectid AND a.restime = a2.restime) b1) b2
"""
	spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q).load()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_03_task02")
        .mode("overwrite").save()
val r = """SELECT ID_Тикета, GROUP_CONCAT(DATE_FORMAT(Status_Time, '%Y-%m-%d %H:%i'),' ', 
case  
when Статус='Зарегистрирован' then 'З'
when Статус='Назначен' then 'Н'
when Статус='В работе' then 'ВР'
when Статус='Решен' then 'Р'
when Статус='Исследование ситуации' then 'ИС'
when Статус='Закрыт' then 'ЗТ'
END, ' ', Группа SEPARATOR '\n') Назначения FROM sem_03_task02
GROUP BY ID_Тикета
"""
var df2 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", r).load()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sem_03_HW")
        .mode("overwrite").save()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)