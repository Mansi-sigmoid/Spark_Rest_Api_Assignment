from flask import Flask,jsonify,json
from pyspark.sql import SparkSession

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

spark = SparkSession.builder.appName(
  'Read CSV Files').getOrCreate()

spark_df= spark.read.csv('/Users/mansigupta/Desktop/PyCharm/pythonProjects/python+spark_FTE/Data/*.csv', sep=',',
          inferSchema=True, header=True)

spark_df.createOrReplaceTempView("Stocks")

#Endpoint name can be more meaningful so it would be easier for other while testing API
#For eg. question endpoint can renamed as "stock_moved_up_or_down"
@app.route('/Ques1',methods=['GET'])
def get_question_1():
    stock_moved_up_highest = spark.sql(
        "WITH added_dense_rank AS (SELECT Date,Stocks_Name,(High-Open)/Open as up_Percentage, dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM Stocks) select Date,Stocks_Name,up_Percentage FROM added_dense_rank where dense_rank=1")
    stock_moved_up_lowest = spark.sql(
        "WITH added_dense_rank AS (SELECT Date,Stocks_Name,(Open-Low)/Open as Down_Percentage, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM Stocks) select Date,Stocks_Name,Down_Percentage FROM added_dense_rank where dense_rank=1")
    stock_moved_up_lowest = stock_moved_up_lowest.withColumnRenamed("Stocks_Name", "maxdown_company")
    stock_moved_up_highest = stock_moved_up_highest.withColumnRenamed("Stocks_Name", "maxup_company")
    SQL_Query_1=stock_moved_up_highest.join(stock_moved_up_lowest, ['Date'], how='inner')
    SQL_Query_1.show(20,False)
    results=SQL_Query_1.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)


@app.route('/Ques2',methods=['GET'])
def get_question_2():
    SQL_Query_2 = spark.sql("WITH added_dense_rank AS (SELECT Date,Stocks_Name,Volume , dense_rank() OVER ( partition by Date order by Volume desc ) as dense_rank FROM data) select Date,CompanyName,Volume FROM added_dense_rank where dense_rank=1")
    results = SQL_Query_2.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)
#DataFrame name is not proper in Ques2 and it will not work as it worked for other problems.
#please update it accordingly.
#data >> stocks


@app.route('/Ques3',methods=['GET'])
def get_question_3():
    SQL_Query_3=spark.sql("with added_previous_close as (select Stock_Name,Open,Date,Close,LAG(Close,1,35.724998) over(partition by Stock_Name order by Date) as previous_close from data ASC) select CompanyName,ABS(previous_close-Open) as max_swing from added_previous_close order by max_swing DESC ").show()
    results = SQL_Query_3.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)
#DataFrame name is not proper in Ques3 and it will not work as it worked for other problems.
#please update it accordingly.
#data >> stocks

@app.route('/Ques4',methods=['GET'])
def get_question_4():
    spark.sql("CREATE TEMP VIEW open_table AS Select Stock_Name, Open from Stocks where Date='2021-07-06T00:00:00'")

    spark.sql("CREATE TEMP VIEW high_table AS Select Stock_Name , Max(High) as High from Stocks group by Stock_Name")
    spark.sql("CREATE TEMP VIEW joined_table AS select t1.Stock_Name, t1.High, t2.Open from high_table t1 Inner join open_table t2 on t1.Stock_Name=t2.Stock_Name")
    spark.sql("select * from joined_table").show()
    SQL_Query_4=spark.sql("Select t1.Stock_Name , t1.High-t1.Open as Maximum_Movement from joined_table t1 where t1.High-t1.Open = (Select Max(t2.High-t2.Open) from joined_table t2)").show()

    results = SQL_Query_4.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)


@app.route('/Ques5',methods=['GET'])
def get_question_5():
    SQL_Query_5=spark.sql("Select Stock_Name, STD(Open) as Standard_Deviations from Stocks group by Stock_Name")
    results = SQL_Query_5.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)


@app.route('/Ques6',methods=['GET'])
def get_question_6():
    x = spark.sql("select Stock_Name,AVG(Open) from Stocks group by Stock_Name ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    y = spark.sql(
        "select distinct * from (select Stock_Name,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY Open) OVER (PARTITION BY Stock_Name) AS Median_UnitPrice from Stocks)")
    results1 = y.toJSON().map(lambda j: json.loads(j)).collect()

    final = {}
    final["mean"] = results
    final["median"] = results1

    return jsonify(final, 200)


@app.route('/Ques7',methods=['GET'])
def get_question_7():
    SQL_Query_7=spark.sql("Select Stock_Name, avg(Volume) as Average_Volume from Stocks group by Stock_Name")
    results = SQL_Query_7.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)


@app.route('/Ques8',methods=['GET'])
def get_question_8():
    SQL_Query_8=spark.sql("Select Stock_Name, avg(Volume) as Average_Volume from Stocks group by Stock_Name order by avg(Volume) DESC limit 1")
    results = SQL_Query_8.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)


@app.route('/Ques9',methods=['GET'])
def get_question9():
    x=spark.sql("Select Stock_Name, Max(High) as Highest,Min(Low) as Lowest from Stocks group by Stock_Name")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)


app.run(host='0.0.0.0', port=5001)

