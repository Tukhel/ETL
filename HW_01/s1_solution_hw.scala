/*
chcp 65001 && spark-shell -i D:\Education\ETL\ETL_new\Seminars\HW_01\s1_solution_hw.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
        .load("D:/Education/ETL/Sem_1_HW.xlsx")
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "task_a")
        .mode("overwrite").save()
		var empl = """
		CREATE TABLE `employees` (
			`Employee_ID` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Name` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			INDEX `Employee_ID` (`Employee_ID`(100)) USING BTREE
		)
		COMMENT='ID сотрудника'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		"""
		var jobs = """
		CREATE TABLE `jobs` (
			`Job_code` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Job` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			INDEX `Job_code` (`Job_code`(100)) USING BTREE
		)
		COMMENT='ID должности'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		"""
		var cities = """
		CREATE TABLE `cities` (
			`City_code` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Home_city` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			INDEX `City_code` (`City_code`(100)) USING BTREE
		)
		COMMENT='ID города'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		"""
import java.sql._;
def sqlexecute(sql: String) = {
	  var conn: Connection = null;
	  var stmt: Statement = null;
	  try {
		  Class.forName("com.mysql.cj.jdbc.Driver");
		  conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?user=root&password=123")
		  stmt = conn.createStatement();
		  stmt.executeUpdate(sql);
		  println(sql + " complete");
	  }		catch {
		  case e: Exception => println("exception caught: " + e);
	  }
	}
	sqlexecute("drop table spark.employees")
	sqlexecute(empl)
	sqlexecute("drop table spark.jobs")
	sqlexecute(jobs)
	sqlexecute("drop table spark.cities")
	sqlexecute(cities)
		df1.select("Employee_ID","Name").distinct().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "employees")
        .mode("append").save()
		df1.select("Job_code","Job").distinct().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "jobs")
        .mode("append").save()
		df1.select("City_code","Home_city").distinct().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "cities")
        .mode("append").save()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)