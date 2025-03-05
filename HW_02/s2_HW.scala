/*
chcp 65001 && spark-shell -i D:\Education\ETL\ETL_new\Seminars\HW_02\s2_HW.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.option("delimiter",",")
        .option("header", "true")
        //.option("encoding", "windows-1251")
        .csv("D:/Education/ETL/ETL_new/Seminars/HW_02/fifa_s2.csv")
		df1=df1
		.withColumn("Club", lower(col("Club")))
		.withColumn("ID",col("ID").cast("int")).dropDuplicates()
		.withColumn("Age",col("Age").cast("int"))
		.withColumn("Overall",col("Overall").cast("int"))
		.withColumn("Potential",col("Potential").cast("int"))
		.withColumn("Height",col("Height").cast("float"))
		.withColumn("Weight",col("Weight").cast("float"))
		.withColumn("Joined",col("Joined").cast("Date"))
		.withColumn("Contract Valid Until",
		to_date(
			when(col("Contract Valid Until").isNotNull, col("Contract Valid Until"))
			.otherwise("01/01/9999"),
			"dd/MM/yyyy"))
		.withColumn("Club",
		when(col("Club").isNotNull,col("Club"))
		.otherwise("without a club"))
		.withColumn("Value",
		when(col("Value").isNotNull,col("Value"))
		.otherwise("NO DATA"))
		.withColumn("International Reputation",
		when(col("International Reputation").isNotNull, col("International Reputation"))
		.otherwise(0).cast("int"))
		//.withColumn("International Reputation",col("International Reputation").cast("int"))
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=123")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "fifa_s2")
        .mode("overwrite").save()
		df1.show()
val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
val df2 = df1.agg(s.head, s.tail:_*)
val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
df_agg_col.show()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)