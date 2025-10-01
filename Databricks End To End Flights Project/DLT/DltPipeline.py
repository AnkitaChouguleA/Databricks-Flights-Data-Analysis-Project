import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bookings Data
@dlt.table(
  name = "silver.stage_bookings"
)
def stage_bookings():

  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
  return df

@dlt.view(
  name = "trans_bookings"
)
def trans_bookings():

  # Corrected to use dlt.read_stream() to establish dependency
  df = dlt.read_stream("silver.stage_bookings")
  df = df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("modifiedDate", current_timestamp())\
    .withColumn("booking_date",to_date(col("booking_date")))\
    .drop("_rescued_data")

  return df


rules = {
  "rule1" : "booking_id IS NOT NULL",
  "rule2" : "passenger_id IS NOT NULL"
}

@dlt.table(
  name = "silver.silver_bookings"
)
@dlt.expect_all_or_drop(rules)

def silver_bookings():

  # Corrected to use dlt.read_stream() to establish dependency
  df = dlt.read_stream("trans_bookings")
  return df


########################################################################################
#flights Data
@dlt.view(
  name = "trans_flights"
)
def trans_flights():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

  df = df.drop("_rescued_data") \
    .withColumn("modifiedDate",current_timestamp())
  
  return df

dlt.create_streaming_table("silver.silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver.silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

########################################################################################
# passengers Data 
@dlt.view(
  name = "trans_passengers"
)
def trans_flights():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

  df = df.drop("_rescued_data") \
    .withColumn("modifiedDate",current_timestamp())
  
  return df

dlt.create_streaming_table("silver.silver_passengers")

dlt.create_auto_cdc_flow(
  target = "silver.silver_passengers",
  source = "trans_passengers",
  keys = ["passenger_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)



############################################################################################

# Airports Data 
@dlt.view(
  name = "trans_airports"
)
def trans_flights():
  df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")

  df = df.drop("_rescued_data") \
    .withColumn("modifiedDate",current_timestamp())
  
  return df

dlt.create_streaming_table("silver.silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver.silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

##################################################################################

# Silver Business View
@dlt.table(
    name = "silver.silver_business"
)

def silver_business():

  df = dlt.read_stream("silver.silver_bookings")\
    .join(dlt.read_stream("silver.silver_flights"), ["flight_id"])\
    .join(dlt.read_stream("silver.silver_passengers"), ["passenger_id"])\
    .join(dlt.read_stream("silver.silver_airports"), ["airport_id"])\
    .select("booking_id", "passenger_id", "flight_id", "amount", "booking_date", "airport_id", "airport_name", "city", "country") \
    .drop("modifiedDate")
  
  return df
