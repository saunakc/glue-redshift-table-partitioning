import sys
import os
import boto3
import base64
import datetime
from boto.kms.exceptions import NotFoundException
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql import functions 
from pyspark.sql import SQLContext

## @params: [TempDir, JOB_NAME]
#args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME', 'OutputFileFormat', 'OutputFilePath', 'PasswordEncrypted'])
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

currentRegion = "us-east-1"

transformSQL = 'SELECT o_orderkey\
, o_custkey\
, o_orderstatus\
, o_totalprice\
, o_orderdate\
, o_orderpriority\
, o_clerk\
, o_shippriority\
, o_comment\
, extract(year from o_orderdate) as order_year\
, LPAD(extract(month from o_orderdate), 2, \'0\') as order_month\
, lpad(extract(day from o_orderdate), 2, \'0\') as order_day\
 FROM awspsatest.demo_master.orders order by o_orderdate'
#transformSQL = 'select sales.salesid, sales.qtysold, event.dateid as event_dateid, event.eventname, event.starttime as event_starttime from sales left join event on sales.eventid = event.eventid'


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Retrieve the job parameters
                 
print "Transform SQL: %s" % transformSQL

tempDir = args['TempDir']

#outputFileFormat = args['OutputFileFormat']
#print "Output file format: %s" % outputFileFormat

#outputFilePath = args['OutputFilePath']
outputFilePath = 's3://bigdatalabsaunak/redshiftdata/parquet/orders/'
print "Output filpath: %s" % outputFilePath

#passwordEncrypted = args['PasswordEncrypted']
#print "Encrypted password for Redshift: %s" % passwordEncrypted

#try:
 #   kms = boto3.client('kms', region_name = currentRegion)
  #  password = kms.decrypt(CiphertextBlob=base64.b64decode(passwordEncrypted))['Plaintext']
#except:
    #print "KMS access failed: exception %s" %sys.exc_info()[1]
 #   print(sys.exc_info()[1])
  #  exit

password = 'Welcome123'
jdbcUrl = "jdbc:redshift://awspsatest.csdiadofcvnr.us-east-1.redshift.amazonaws.com:8200/awspsatest?user=superuser&password=%s" %password
print "JDBC Url: %s" % jdbcUrl

sql_context = SQLContext(sc)
df = sql_context.read.format("com.databricks.spark.redshift")\
                .option("url", jdbcUrl) \
                .option("query", transformSQL) \
                .option("aws_iam_role", "arn:aws:iam::989581143570:role/redshiftrole") \
                .option("tempdir", tempDir) \
                .load()
    
df.printSchema()

#final_df = df.repartition("order_year", "order_month", "order_day")
#final_df.printSchema()
#final_df.write.mode("overwrite").parquet(outputFilePath)
df.write.partitionBy("order_year", "order_month", "order_day").format("parquet").save(outputFilePath)



