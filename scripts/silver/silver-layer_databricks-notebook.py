# Databricks notebook source
# import all
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA ACCESS USING 

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.<your-storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<your-storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<your-storage-account>.dfs.core.windows.net", "<your-application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<your-storage-account>.dfs.core.windows.net", "<your-secret-value>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<your-storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<your-directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading Data

# COMMAND ----------

df_regions = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/regions")


# COMMAND ----------

df_st_regions = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/state-regions")

# COMMAND ----------

df_brokers = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/brokers")

# COMMAND ----------

df_coverage = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/coverages")

# COMMAND ----------

df_participants = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/participants")

# COMMAND ----------

df_policies = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/policies")

# COMMAND ----------

df_products = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/products")

# COMMAND ----------

df_claims_announcement = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/claims-announcements")

# COMMAND ----------

df_claims_payments = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/claims-payments")

# COMMAND ----------

df_claims_reserves = spark.read.format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@insuranceclaimsdatalake.dfs.core.windows.net/claims-reserves")

# COMMAND ----------

df_st_regions.display()

# COMMAND ----------

df_regions.display() 

# COMMAND ----------

df_brokers.display()  

# COMMAND ----------

df_coverage.display()  

# COMMAND ----------

df_participants.display()  

# COMMAND ----------

df_policies.display()

# COMMAND ----------

df_products.display() 

# COMMAND ----------

df_claims_announcement.display()  

# COMMAND ----------

df_claims_payments.display()  

# COMMAND ----------

df_claims_reserves.display()  

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA TRANSFORMATION

# COMMAND ----------

# REGIONS
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate or 

# df_unique = df_regions.select(
#   df_regions["id"],
# ).distinct()
# df_unique.display()

# Checking for NULLs
# >> Value must no be NULL
# Expectation: No Result 
# df_regions = df_regions.filter(
#         col("name").isNull() 
#         | col("county").isNull()
#         | col("state_code").isNull()
#         | col("state").isNull()
#         | col("type").isNull()
#         | col("latitude").isNull()
#         | col("longitude").isNull()
#         | col("area_code").isNull()
#         | col("population").isNull()
#         | col("households").isNull()
#         | col("median_income").isNull()
#         | col("land_area").isNull()
#         | col("water_area").isNull()
#         | col("time_zone").isNull()
#     )

df_regions.display()

# COMMAND ----------

# STATE REGIONS
# Checking Standardization & Consistency
# Checking for NULLs
# >> Value must no be NULL
# Expectation: No Result 
# df_st_regions = df_st_regions.filter(
#         col("State Code").isNull() 
#         | col("State").isNull()
#         | col("Region").isNull()
#     )

df_st_regions = df_st_regions.withColumnRenamed("State Code", "state_code")\
             .withColumnRenamed("State", "state")\
             .withColumnRenamed("Region", "region")    

df_st_regions.display()



# COMMAND ----------

# BROKERS
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_brokers = df_brokers.select(
#   df_brokers.BrokerID,
# ).distinct()

# df_brokers = df_brokers.na.drop()

#df_brokers.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result 
# df_brokers = df_brokers.filter(
#         col("BrokerCode").isNull() 
#         | col("BrokerFullName").isNull()
#         | col("DistributionNetwork").isNull()
#         | col("DistributionChannel").isNull()
#         | col("CommissionScheme").isNull()
#     )
#df_brokers.display()



df_brokers = df_brokers.withColumnRenamed("BrokerID", "broker_id")\
             .withColumnRenamed("BrokerCode", "broker_code")\
             .withColumnRenamed("BrokerFullName", "broker_fullname")\
             .withColumnRenamed("DistributionNetwork", "distribution_network")\
             .withColumnRenamed("DistributionChannel", "distribution_channel")\
             .withColumnRenamed("CommissionScheme", "commission_scheme")         

df_brokers = df_brokers.na.drop()

df_brokers.display()

# COMMAND ----------

# COVERAGES
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_coverage = df_coverage.select(
#   df_coverage.cover_id,
# ).distinct()

# #df_coverage = df_coverage.na.drop()

# df_coverage.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result 
# df_coverage = df_coverage.filter(
#         col("CoverCode").isNull() 
#         | col("RenewalType").isNull()
#         | col("Room").isNull()
#         | col("Participation").isNull()
#         | col("ProductCategory").isNull()
#         | col("PremiumMode").isNull()
#         | col("ProductDistribution").isNull()
#     )
#df_coverage.display()



df_coverage = df_coverage.withColumnRenamed("CoverID", "cover_id")\
             .withColumnRenamed("CoverCode", "cover_code")\
             .withColumnRenamed("RenewalType", "renewal_type")\
             .withColumnRenamed("Room", "room")\
             .withColumnRenamed("Participation", "participation")\
             .withColumnRenamed("ProductCategory", "product_category")\
             .withColumnRenamed("PremiumMode", "premium_mode")\
             .withColumnRenamed("ProductDistribution", "product_distribution")                   

# df_coverage = df_coverage.na.drop()

df_coverage.display()

# COMMAND ----------

# PARTICIPANT
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_participants = df_participants.select(
#   df_participants.ParticipantID,
#   df_participants.RegionID
# ).distinct()

# #df_participants = df_participants.na.drop()

# df_participants.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_participants = df_participants.filter(
#         col("ParticipantID").isNull() 
#         | col("ParticipantCode").isNull()
#         | col("LastName").isNull()
#         | col("FirstName").isNull()
#         | col("BirthDate").isNull()
#         | col("Gender").isNull()
#         | col("ParticipantType").isNull()
#         | col("RegionID").isNull()
#         | col("MaritalStatus").isNull()
#     )



df_participants = df_participants.withColumnRenamed("ParticipantID", "participant_id")\
             .withColumnRenamed("ParticipantCode", "participant_code")\
             .withColumnRenamed("LastName", "lastname")\
             .withColumnRenamed("FirstName", "firstname")\
             .withColumnRenamed("BirthDate", "birthdate")\
             .withColumnRenamed("Gender", "gender")\
             .withColumnRenamed("ParticipantType", "participant_type")\
             .withColumnRenamed("RegionID", "region_id")\
             .withColumnRenamed("MaritalStatus", "marital_status")\
             .withColumn("fullname", concat(col("firstname"), lit(" "), col("lastname")))                             
             
df_participants = df_participants.na.fill("N/A", subset=["firstname", "lastname", "gender", "participant_type", "marital_status", "fullname"])

df_participants = df_participants.fillna({'birthdate': '1900-01-01'})

df_participants.display()

# COMMAND ----------

# POLICIES
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_policies = df_policies.select(
#   df_policies.PolicyID,
#   df_policies.RegionID
# ).distinct()

# #df_policies = df_policies.na.drop()

# df_policies.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_policies = df_policies.filter(
#         col("PolicyID").isNull() 
#         | col("PolicyCode").isNull()
#         | col("PolicyInceptionDate").isNull()
#         | col("CancelationDate").isNull()
#         | col("PolicyStartDate").isNull()
#         | col("PolicyExpirationDate").isNull()
#         | col("RenewalMonth").isNull()
#         | col("AnnualizedPolicyPremium").isNull()
#         | col("PolicyStatus").isNull()
#         | col("CustomerID").isNull()
#         | col("InsuredID").isNull() 
#         | col("ProductID").isNull() 
#         | col("BrokerID").isNull()   
#     )



df_policies = df_policies.withColumnRenamed("PolicyID", "policy_id")\
             .withColumnRenamed("PolicyCode", "policy_code")\
             .withColumnRenamed("PolicyInceptionDate", "policy_inception_date")\
             .withColumnRenamed("CancelationDate", "cancelation_date")\
             .withColumnRenamed("PolicyStartDate", "policy_start_date")\
             .withColumnRenamed("PolicyExpirationDate", "policy_expiration_date")\
             .withColumnRenamed("RenewalMonth", "renewal_month")\
             .withColumnRenamed("AnnualizedPolicyPremium", "annualized_policy_premium")\
             .withColumnRenamed("PolicyStatus", "policy_status")\
             .withColumnRenamed("CustomerID", "customer_id")\
             .withColumnRenamed("InsuredID", "insured_id")\
             .withColumnRenamed("ProductID", "product_id")\
             .withColumnRenamed("BrokerID", "broker_id")
                                

df_policies.display()

# COMMAND ----------

# PRODUCTS
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_products = df_products.select(
#   df_products.ProductID,
# ).distinct()

# df_products.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_products = df_products.filter(
#         col("ProductID").isNull() 
#         | col("ProductCategory").isNull()
#         | col("ProductSubCategory").isNull()
#         | col("Product").isNull() 
#     )

# df_products.display()


df_products = df_products.withColumnRenamed("ProductID", "product_id")\
             .withColumnRenamed("ProductCategory", "product_category")\
             .withColumnRenamed("ProductSubCategory", "product_sub_category")\
             .withColumnRenamed("Product", "product")
                                

df_products.display()

# COMMAND ----------

# CLAIMS ANNOUNCEMENT
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_claims_announcement = df_claims_announcement.select(
#   df_claims_announcement.ClaimID,
#   df_claims_announcement.ClaimCode
# ).distinct()

# #df_claims_announcement = df_claims_announcement.na.drop()

# df_claims_announcement.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_claims_announcement = df_claims_announcement.filter(
#         col("ClaimID").isNull() 
#         | col("ClaimCode").isNull()
#         | col("PolicyID").isNull()
#         | col("PolicyCode").isNull()
#         | col("AnnouncementDate").isNull()
#         | col("EventDate").isNull()
#         | col("ClosingDate").isNull()
#         | col("LastForecastAmount").isNull()
#         | col("InsuredID").isNull() 
#         | col("ProductID").isNull() 
#         | col("BrokerID").isNull()   
#     )

df_claims_announcement = df_claims_announcement.withColumnRenamed("ClaimID", "claim_id")\
             .withColumnRenamed("ClaimCode", "claim_code")\
             .withColumnRenamed("PolicyID", "policy_id")\
             .withColumnRenamed("PolicyCode", "policy_code")\
             .withColumnRenamed("AnnouncementDate", "announcement_date")\
             .withColumnRenamed("EventDate", "event_date")\
             .withColumnRenamed("ClosingDate", "closing_date")\
             .withColumnRenamed("LastForecastAmount", "last_forecast_amount")\
             .withColumnRenamed("BrokerID", "broker_id")\
             .withColumnRenamed("InsuredID", "insured_id")\
             .withColumnRenamed("ProductID", "product_id")

df_claims_announcement.display()

# COMMAND ----------

# CLAIMS RESERVES
# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_claims_reserves = df_claims_reserves.filter(
#         col("ClaimID").isNull() 
#         | col("ClaimCode").isNull()
#         | col("PolicyID").isNull()
#         | col("AnnouncementDate").isNull()
#         | col("ClosingDate").isNull()
#         | col("CoverID").isNull()
#         | col("ProvisionAmount").isNull()
#         | col("ProvisionDate").isNull()
#         | col("InsuredID").isNull() 
#         | col("ProductID").isNull() 
#         | col("BrokerID").isNull()   
#     )

df_claims_reserves = df_claims_reserves.withColumnRenamed("ClaimID", "claim_id")\
             .withColumnRenamed("ClaimCode", "claim_code")\
             .withColumnRenamed("PolicyID", "policy_id")\
             .withColumnRenamed("AnnouncementDate", "announcement_date")\
             .withColumnRenamed("ClosingDate", "closing_date")\
             .withColumnRenamed("CoverID", "cover_id")\
             .withColumnRenamed("ProvisionAmount", "provision_amount")\
             .withColumnRenamed("ProvisionDate", "provision_date")\
             .withColumnRenamed("BrokerID", "broker_id")\
             .withColumnRenamed("InsuredID", "insured_id")\
             .withColumnRenamed("ProductID", "product_id")

df_claims_reserves.display()

# COMMAND ----------

# CLAIMS PAYMENTS
# Checking Standardization & Consistency
# >> Value must no be NULL
# Expectation: No Duplicate ID

# df_claims_payments = df_claims_payments.select(
#   df_claims_payments.ClaimPaymentCode,
# ).distinct()

# #df_claims_payments = df_claims_payments.na.drop()

# df_claims_payments.display()

# # Checking for NULLs
# # >> Value must no be NULL
# # Expectation: No Result or Null is replaced with N/A 
# df_claims_payments = df_claims_payments.filter(
#         col("ClaimPaymentCode").isNull() 
#         | col("ClaimID").isNull() 
#         | col("ClaimCode").isNull()
#         | col("PolicyID").isNull()
#         | col("AnnouncementDate").isNull()
#         | col("EventDate").isNull()
#         | col("ClosingDate").isNull()
#         | col("CoverID").isNull()
#         | col("PaymentDate").isNull()
#         | col("PaymentAmount").isNull()
#         | col("InsuredID").isNull() 
#         | col("ProductID").isNull() 
#         | col("BrokerID").isNull()   
#     )

df_claims_payments = df_claims_payments.withColumnRenamed("ClaimPaymentCode", "payment_code")\
             .withColumnRenamed("ClaimID", "claim_id")\
             .withColumnRenamed("ClaimCode", "claim_code")\
             .withColumnRenamed("PolicyID", "policy_id")\
             .withColumnRenamed("AnnouncementDate", "announcement_date")\
             .withColumnRenamed("EventDate", "event_date")\
             .withColumnRenamed("ClosingDate", "closing_date")\
             .withColumnRenamed("CoverID", "cover_id")\
             .withColumnRenamed("PaymentDate", "payment_date")\
             .withColumnRenamed("PaymentAmount", "payment_amount")\
             .withColumnRenamed("BrokerID", "broker_id")\
             .withColumnRenamed("InsuredID", "insured_id")\
             .withColumnRenamed("ProductID", "product_id")
         

df_claims_payments.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVING DATA TO SILVER

# COMMAND ----------

# REGION

df_regions.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/regions').save()

display(df_regions)

# COMMAND ----------

# STATE REGIONS

df_st_regions.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/state-regions').save()


display(df_st_regions)

# COMMAND ----------

# BROKERS

df_brokers.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/brokers').save()


display(df_brokers)

# COMMAND ----------

# COVERAGES

df_coverage.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/coverages').save()

display(df_coverage)

# COMMAND ----------

# PARTICIPANTS

df_participants.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/participants').save()

display(df_participants)

# COMMAND ----------

# POLICIES

df_policies.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/policies').save()

display(df_policies)

# COMMAND ----------

# PRODUCTS

df_products.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/products').save()

display(df_products)

# COMMAND ----------


# CLAIMS ANNOUNCEMENTS
df_claims_announcement.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/claims-announcement').save()


# CLAIMS RESERVES
df_claims_reserves.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/claims-reserves').save()


# CLAIMS RESERVES
df_claims_payments.write.format("Parquet").mode("overwrite").option('path', 'abfss://silver@insuranceclaimsdatalake.dfs.core.windows.net/claims-payments').save()


display(df_claims_announcement)

display(df_claims_reserves)


display(df_claims_payments)

