# Databricks notebook source
# Author: Lance de Hoog
# Last updated: 24-10-2022
# Description: Used for the conversion of a XAF file to the desired VAT mapping defined
# You need the following scripts and data:
# The flattened XAF File as Parquet format from AFAS for the relevant company containing supplier information

# COMMAND ----------

from datetime import datetime as dt
import os
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pyspark.pandas as ks
import re

# COMMAND ----------

# In UsingPipelineParameters notebook we have the function create_widgets() for creating widgets and function get_widget_value for linking pipelineparameters to python-variables.

dbutils.widgets.removeAll()
create_widgets()

# COMMAND ----------

# Define variables from the widgets

folder_name = #container name where parquet file is located#
work_date =   #date as folder name where the parquet file is retrieved#
upload_date = dt.today().strftime("%Y-%m-%d")


# Define used filepaths

filepath_silver = f'/mnt/silver_{folder_name}/source/{work_date}/'
filepath_gold = f'/mnt/gold_{folder_name}/source/{upload_date}/'


# Define output filename

filename = f'VAT_Table_Output_{upload_date}_{folder_name}'

# COMMAND ----------

# Read available files in folder

def read_function(filepath, filename):
  df = spark.read.format('parquet') \
                .option('header',True) \
                .option('multiLine', True) \
                .option('encoding','ISO-8859-1') \
                .option('storeAssignmentPolicy', 'legacy') \
                .load(filepath+filename)
  return df

# COMMAND ----------

# List available files (sorted)

files = os.listdir('/dbfs'+filepath_silver)
files = sorted(files, key=str.casefold)
files

# COMMAND ----------

# Input filenames
# The files are created by flattening the XAF to parquet files in Data Factory

df_CustomersSuppliers = spark.read.parquet(f'{filepath_silver}{folder_name}customerSupplier.parquet')
df_VATCode            = spark.read.parquet(f'{filepath_silver}{folder_name}vatCodes.parquet')
df_main               = spark.read.parquet(f'{filepath_silver}{folder_name}transactionLines.parquet')
df_check              = spark.read.parquet(f'{filepath_silver}{folder_name}transactions_total.parquet')
df_CompanyHeader      = spark.read.parquet(f'{filepath_silver}{folder_name}company_header.parquet')
df_generalLedger      = spark.read.parquet(f'{filepath_silver}{folder_name}generalLedger.parquet')

# COMMAND ----------

# Check to see if the total of all the rows in the parquet file sums up to the total amount as given in the XAF file

Credit = df_main.filter(df_main.amntTp == 'C').agg({'amnt': 'sum'})
Debit = df_main.filter(df_main.amntTp == 'D').agg({'amnt': 'sum'})

print(Credit.collect())
print(Debit.collect())
display(df_check)


# COMMAND ----------

tradeGL = [16000, 16001, 36000, 36001, 38360]        # Main Creditor and Debtor GL's where costs/vat is booked against in transaction (impacting the balance)
df_main = df_main.na.fill(0,['vatAmnt','curAmnt'])   # Fill out the null values with 0 to avoid errors in calculations
col_order = df_main.columns                          # Used to reorder the columns after adding columns

df_memo_vat = df_main.filter((F.col('jrnTp') == 'M') & (F.col('vatID').isNotNull()))                           # Memo transactions that impact the VAT / filtering out the transactions where VAT code is null
df_memo_vat = df_memo_vat.select('nr').distinct()                                                              # Creating a list of all the trasaction numbers, using the transaction ID 'nr'
df_memo = df_main.filter(F.col('jrnTp') == 'M').join(df_memo_vat, on= ['nr'], how= 'inner').select(col_order)  # Joining to the main memo transaction table with the 'nr', thus filtering out all the transactions that are relevant to the VAT bookings 

df_main_ps = df_main.filter(F.col('jrnTp').isin('P','S'))                                                      # Purchase /  Sales journal
df_main = df_main_ps.union(df_memo)                                                                            # Appending the filtered memo journal to the purchase / sales journal


# Following the way of the bookings to define clearly if a transaction is a reversal 'negative' or a normal booking 'positive'

df_main = df_main.withColumn('saldo', F.when( (F.col('jrnTp') == 'P') & (F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'C') , F.col('amnt')).
                                             when( (F.col('jrnTp') == 'P') & (F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'D') , F.col('amnt')*-1).
                                                  when( (F.col('jrnTp').isin(['S','M'])) & (F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'C') , F.col('amnt')*-1).
                                                       when( (F.col('jrnTp').isin(['S','M'])) & (F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'D') , F.col('amnt')).
                                                            when( (F.col('jrnTp') == 'P') & (~F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'C') , F.col('amnt')*-1).
                                                                 when( (F.col('jrnTp') == 'P') & (~F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'D'), F.col('amnt')).
                                                                      when( (F.col('jrnTp').isin(['S','M'])) & (~F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'C') , F.col('amnt')).
                                                                           when( (F.col('jrnTp').isin(['S','M'])) & (~F.col('accID').isin(tradeGL)) & (F.col('amntTp') == 'D') , F.col('amnt')*-1).
                                                                                otherwise(0)
                            )


# Following the way of the bookings to define clearly if a transaction is a reversal 'negative' or a normal booking 'positive' for VAT

df_main = df_main.withColumn('saldo_vat', F.when( (F.col('vatAmntTp') == 'D') & (F.col('saldo') <= 0), F.col('vatAmnt')*-1).
                             when( (F.col('vatAmntTp') == 'D') & (F.col('saldo') >= 0), F.col('vatAmnt')).
                             when( (F.col('vatAmntTp') == 'C') & (F.col('saldo') <= 0), F.col('vatAmnt')*-1).
                             when( (F.col('vatAmntTp') == 'C') & (F.col('saldo') >= 0), F.col('vatAmnt')).
                             otherwise(0))


# Using the currency amount to calculate the foreign currency for both the amounts and VAT amounts

df_main = df_main.withColumn('saldo_fx', F.when( (F.col('saldo') <= 0) , F.col('curAmnt')*-1 ).otherwise(F.col('curAmnt')))
df_main = df_main.withColumn('saldo_vat_fx', F.col('saldo_vat') * F.abs((F.col('saldo_fx') / F.col('saldo'))) )

# COMMAND ----------

mainCompanyName = df_CompanyHeader.collect()[0][1]                       # Retrieving the used entity's name
df_main = df_main.withColumn("mainCompanyName", F.lit(mainCompanyName))  # Adding the entity's name to the main table

df_CompanyHeader = df_CompanyHeader.join(df_CustomersSuppliers['custSupID', 'custSupName','custTransStreetname','custTransNumber','CustTransPostalCode','custTransCity','CustTransCountry'], df_CompanyHeader.mainCompanyName == df_CustomersSuppliers.custSupName)  # Adding the shipping information of the Entity

# COMMAND ----------

# Creatinging views to interact with in spark.sql

df_CustomersSuppliers.createOrReplaceTempView('cus')
df_VATCode.createOrReplaceTempView('vatc')
df_main.createOrReplaceTempView('main')
df_CompanyHeader.createOrReplaceTempView('head')
df_generalLedger.createOrReplaceTempView('gl')

# COMMAND ----------

#Define four types of dataframes based on the XAF output file made by tool

df_maingross = df_main.filter(df_main['accID'].isin(tradeGL) & df_main['jrnTp'].isin(['P','S']))  # Gross/Total amounts dataframe containing the full amount of a transaction. These GL's were defined earlier.
df_maingross_memo = df_main.filter(df_main['accID'].isin(tradeGL) & df_main['jrnTp'].isin('M'))   # Gross/Total amounts dataframe for the Memo bookings


# Grouping the Memo bookings, as sometimes these totals are split-up in the same booking

df_maingross_memo = df_maingross_memo.groupby('desc', 'jrnID', 'jrnTp', 'nr', 'periodNumber', 'sourceID', 'trDt', 'accID', 'custSupID', 'effDate', 'vatAmntTp', 'vatID', 'vatPerc', 'curCode', 'mainCompanyName') \
                                     .agg(F.sum('amnt').alias('amnt'),
                                          F.sum('vatAmnt').alias('vatAmnt'),
                                          F.sum('curAmnt').alias('curAmnt'),
                                          F.sum('saldo').alias('saldo'),
                                          F.sum('saldo_vat').alias('saldo_vat'),
                                          F.sum('saldo_fx').alias('saldo_fx'),
                                          F.sum('saldo_vat_fx').alias('saldo_vat_fx'),
                                          F.max('transDesc').alias('transDesc'),
                                          F.max('transNr').alias('transNr'),
                                          F.max('docRef').alias('amntTp'),
                                          F.max('docRef').alias('docRef'),
                                          F.max('invRef').alias('invRef')).select(df_maingross.columns)


df_maingross = df_maingross.union(df_maingross_memo) # Appending to the main Gross dataframe

df_maingross.createOrReplaceTempView('xg')           # Creating a view for the gross transactions, these form the the base of the transacitons in the eventual dataframe


# Creating a dataframe for the bookinglines unrelated to the VAT or Gross amount, thus all the costs/balance booking that is relevant for analytics of costs and expenditure

df_mainbase =  df_main.filter((df_main['accID'] < 14999) | (df_main['accID'] > 15999) & (~df_main['accID'].isin(tradeGL) & df_main['jrnTp'].isin(['P','S','M'])))
df_mainbase.createOrReplaceTempView('xa')

# Creating a dataframe for the bookinglines related to the VAT
df_mainvat = df_main.filter((df_main['accID'] > 14999) & (df_main['accID'] < 16000) & df_main['jrnTp'].isin(['P','S','M'])) #Xaf VAT amount dataframe
df_mainvat.createOrReplaceTempView('xv')

# COMMAND ----------

# Query and form the main dataframe used for the output
# For this we use the gross amounts from the XAF ouput and enrich with information

df_xafout_gross = spark.sql("""

SELECT DISTINCT
head.mainCompanyName                                                        AS CompanyID
,CAST(xa.transNr AS INT)                                                    AS RecordID
,xg.docRef                                                                  AS DocumentID
,CAST(xg.nr AS INT)                                                         AS OrderID
,xg.jrnTp                                                                   AS DocumentType
,xg.jrnID                                                                   AS JournalCode
,xg.desc                                                                    AS JournalName
,ROUND(xg.saldo,2)                                                          AS VAT_TotalAmount_LocalCurrency
,ROUND(xa.saldo+xa.saldo_vat,2)                                             AS VAT_GrossAmount_LocalCurrency
,ROUND(xa.saldo,2)                                                          AS VAT_BaseAmount_LocalCurrency     
,ROUND(xa.saldo_vat,2)                                                      AS VAT_Amount_LocalCurrency     
,head.curCode                                                               AS CurrencyCode_LocalCurrency
,ROUND(xg.saldo_fx,2)                                                       AS VAT_BaseAmount_ForeignCurrency
,ROUND(xa.saldo_vat_fx,2)                                                   AS VAT_Amount_ForeignCurrency          
,xg.curCode                                                                 AS CurrencyCode_ForeignCurrency    
,0                                                                          AS VAT_Amount_Deducted
,xa.vatPerc                                                                 AS VAT_Percentage
,xg.trDt                                                                    AS PostingDate_Invoice
,xg.effDate                                                                 AS DocumentDate_Invoice
,YEAR(xg.trDt)                                                              AS FinancialYear
,xa.transDesc                                                               AS TransactionDescription
,xa.accID                                                                   AS COST_LedgerAccountCode
,xv.accID                                                                   AS VAT_LedgerAccountCode
,CASE WHEN cus.custSupTp = 'S' THEN head.mainTaxRegIdent
      WHEN cus.custSupTp = 'C' THEN cus.custTaxRegIdent
      ELSE 'Error' 
      END                                                                   AS VAT_Buyer_VAT_Registration_Nr
      
,CASE WHEN cus.custSupTp = 'S' THEN cus.custTaxRegIdent 
      WHEN cus.custSupTp = 'C' THEN head.mainTaxRegIdent
      ELSE 'Error' 
      END                                                                   AS VAT_Seller_VAT_Registration_Nr
      
,xa.vatID                                                                   AS VAT_ItemGroup
,vatc.vatDesc                                                               AS VAT_Group
,xg.sourceID                                                                AS UserID
,CASE WHEN cus.custSupTp = 'S' THEN head.custSupID 
      WHEN cus.custSupTp = 'C' THEN cus.custSupID
      ELSE 'Error' 
      END                                                                   AS BuyVendor_SellCustomer_Code
      
,CASE WHEN cus.custSupTp = 'C' THEN head.custSupID 
      WHEN cus.custSupTp = 'S' THEN cus.custSupID
      ELSE 'Error' 
      END                                                                   AS PayVendor_BillCustomer_Code
      
,cus.custSupName                                                            AS PayVendor_BillCustomer_Name
      
,CONCAT(cus.custBillingStreetname," ", cus.custBillingNumber)               AS PayVendor_BillCustomer_Address
      
,cus.custBillingCity                                                        AS PayVendor_BillCustomer_City
      
,cus.custBillingPostalCode                                                  AS PayVendor_BillCustomer_PostCode
      
,CASE WHEN cus.custSupTp = 'S' THEN head.mainTaxRegistrationCountry
      WHEN cus.custSupTp = 'C' THEN cus.custBillingCountry
      ELSE 'Error' 
      END                                                                   AS PayVendor_BillCustomer_Country

,CASE WHEN cus.custSupTp = 'S' AND head.CustTransCountry IS null THEN head.mainTaxRegistrationCountry
      WHEN cus.custSupTp = 'S' AND head.CustTransCountry IS NOT null THEN head.CustTransCountry
      WHEN cus.custSupTp = 'C' AND cus.CustTransCountry IS null THEN cus.custBillingCountry
      WHEN cus.custSupTp = 'C' AND cus.CustTransCountry IS NOT null THEN cus.CustTransCountry
      ELSE 'Error' 
      END                                                                   AS Ship_to_Code
      
,CASE WHEN cus.custSupTp = 'S' THEN cus.custSupName
      WHEN cus.custSupTp = 'C' THEN cus.custSupName
      ELSE 'Error' 
      END                                                                   AS Ship_to_Name
      
,CASE WHEN head.CustTransCountry IS null THEN CONCAT(cus.custBillingStreetname,' ',cus.custBillingNumber)
      WHEN head.CustTransCountry IS NOT null THEN CONCAT(cus.custTransStreetname,' ',cus.custTransNumber)
      ELSE 'Error' 
      END                                                                    AS Ship_to_Address
      
,CASE WHEN head.CustTransCountry IS null THEN cus.custBillingCity
      WHEN head.CustTransCountry IS NOT null THEN cus.custTransCity
      ELSE 'Error' 
      END                                                                  AS Ship_to_City
      
,CASE WHEN head.CustTransCountry IS null THEN cus.custBillingPostalCode
      WHEN head.CustTransCountry IS NOT null THEN cus.custTransPostalCode
      ELSE 'Error' 
      END                                                                    AS Ship_to_PostCode
      
,CASE WHEN cus.custSupTp = 'S' AND head.CustTransCountry IS null THEN head.mainTaxRegistrationCountry
      WHEN cus.custSupTp = 'S' AND head.CustTransCountry IS NOT null THEN head.CustTransCountry
      WHEN cus.custSupTp = 'C' AND cus.CustTransCountry IS null THEN cus.custBillingCountry
      WHEN cus.custSupTp = 'C' AND cus.CustTransCountry IS NOT null THEN cus.CustTransCountry
      ELSE 'Error' 
      END                                                                   AS Ship_to_Country
      
,CASE WHEN cus.custSupTp = 'C' AND head.CustTransCountry IS null THEN head.mainTaxRegistrationCountry
      WHEN cus.custSupTp = 'C' AND head.CustTransCountry IS NOT null THEN head.CustTransCountry
      WHEN cus.custSupTp = 'S' AND cus.CustTransCountry IS null THEN cus.custBillingCountry
      WHEN cus.custSupTp = 'S' AND cus.CustTransCountry IS NOT null THEN cus.CustTransCountry
      ELSE 'Error' 
      END                                                                      AS Ship_From_Country
      
FROM xg
LEFT JOIN xa ON xa.nr = xg.nr
LEFT JOIN xv ON xv.nr = xg.nr
LEFT JOIN head ON head.mainCompanyName = xg.mainCompanyName
LEFT JOIN cus ON cus.custSupID = xg.custSupID --JOIN Suppl/Cust information--
LEFT JOIN vatc ON vatc.vatID = xa.vatID --JOIN VAT code information--

""")

# COMMAND ----------

# Update the view of the main xaf output to common data format
  
df_xafout_gross.createOrReplaceTempView('xg')

# COMMAND ----------

# Check for duplicate rows (VATTRN_RecordID must be unique in the final df)

display(df_xafout_gross.groupBy('VATTRN_RecordID', 'VATTRN_DocumentType').count().filter(("count > 1")))

# COMMAND ----------

def write_parquet_file_spark(df, destination, filename):
  """
  Function to write dataframe into parquet file using spark (credential passthrough clusters)
  @param df: spark dataframe that needs to be converted
  @param destination: location to store the parquet-file
  @param filename: filename of the parquet
  @return: no return
  """
  # Create single partition of spark instance and write parquet as a folder (spark default)
  df_xafout_gross.repartition(1).write.mode('overwrite').parquet(os.path.join(filepath_gold, 'tmp_' + filename))

  # Select the parquet-file out of the 'spark'-folder
  all_files = dbutils.fs.ls(os.path.join(filepath_gold, 'tmp_' + filename))
  all_parquet_files = []
  for file in all_files:
    if file.path[-8:]=='.parquet' and file.name[0] != '_':
      all_parquet_files.append(file.path)

  assert len(all_parquet_files)==1,f"There are more than one parquet-files (i.e. {len(all_parquet_files)}) created in the spark-folder. You should make sure you repartition the spark dataframe into one."

  # Copy the parquet-file out of the spark folder to the desired location
  dbutils.fs.cp(all_parquet_files[0], os.path.join(filepath_gold, filename))

  # Remove the spark-folder
  dbutils.fs.rm(os.path.join(filepath_gold, 'tmp_' + filename), True)

# COMMAND ----------

#Save final df to parquet & CSV to gold container

write_csv_file(df_xafout_gross, filepath_gold, f'{filename}.csv', '|')
write_parquet_file_spark(df_xafout_gross, filepath_gold, filename)
