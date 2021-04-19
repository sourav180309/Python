# Databricks notebook source
# MAGIC  %md ####This is to maintain all the common configs and credentials for SPoT Databricks jobs (both for prod and edp)

# COMMAND ----------

import json
import time, datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
timestr=time.strftime('%Y_%m_%d_%H%m')

# COMMAND ----------

def spotconfig():
      
            notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            #print(notebook_info)
            try:
              env = notebook_info['tags']['browserHostName']
            except KeyError:
              env = notebook_info['extraContext']['api_url']
         
            print("Loading configurations for --> "+env)
            if "tfsedp" in env or "tfstest" in env:
              env = "tfsedp"
            if "tfsprod" in env:
              env = "tfsprod"
              print(env)
            global oce_username
            global oce_password
            global oracle_url
            global url
            global url_stg
            global url_rpt_edsr
            global url_spot_log
            global temp
            global delta_path
            global gla_user
            global gla_password
            
            if env=="tfsedp":
              ##########Source OCE (Oracle database) Connections
              oce_username = dbutils.secrets.get('lsg_finance', 'oce_user') 
              oce_password = dbutils.secrets.get('lsg_finance', 'oce_pass')
              #oce_password = dbutils.secrets.get('lsg_finance', 'oce_pass_uat')
              oracle_url = f"jdbc:oracle:thin:username/password@//awsu1-10lhyd03p.amer.thermo.com" + f":1521/HYSA02P_PRIM"##This is Prod connection
              ##########Redshift Database Connections
              dbutils.widgets.text("redshift_instance", "rs-cdwdm-tst.c1rrn5bglols.us-east-1.redshift.amazonaws.com")
              dbutils.widgets.text("redshift_instance_prd","cdwprd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com")
              dbutils.widgets.text("database", "rscdwdm")
              dbutils.widgets.text("database_prd", "cdwprd")
              Suser = dbutils.secrets.get('lsg_finance', 'spotRedshiftuser')
              Spassword = dbutils.secrets.get('lsg_finance', 'spotRedshiftpass')
              gla_user = dbutils.secrets.get('lsg_finance', 'spot_gla_user') 
              gla_password = dbutils.secrets.get('lsg_finance', 'spot_gla_pass')
              log_user = dbutils.secrets.get('lsg_finance', 'log_user') 
              log_user_pass = dbutils.secrets.get('lsg_finance', 'log_user_pass')
              stg_user = dbutils.secrets.get('lsg_finance', 'spot_stg_user') 
              stg_password = dbutils.secrets.get('lsg_finance', 'spot_stg_pass')
              url_stg = f"jdbc:redshift://" + getArgument("redshift_instance") + f":5439/"+ getArgument("database") + f"?user=" + stg_user + f"&password=" + stg_password
              url = f"jdbc:redshift://" + getArgument("redshift_instance") + f":5439/"+ getArgument("database") + f"?user=" + gla_user + f"&password=" + gla_password
              url_rpt_edsr = f"jdbc:redshift://" + getArgument("redshift_instance_prd") + f":5439/"+ getArgument("database_prd") + f"?user=" + Suser + f"&password=" + Spassword
              url_spot_log = f"jdbc:redshift://" + getArgument("redshift_instance") + f":5439/"+ getArgument("database") + f"?user=" + log_user + f"&password=" + log_user_pass
              temp = "s3a://tfsdl-lsg-spot-test/" + f"databricks/data"
              delta_path = "s3a://tfsdl-lsg-spot-test/delta/"
              ##########File path and other variables and params
              timestr=time.strftime('%Y_%m_%d_%H%m')
              dbutils.widgets.text("s3path", "s3a://tfsdl-lsg-spot-test/plnr_landing/")
            
            if env=="tfsprod":
              ##########Source OCE (Oracle database) Connections
              oce_username = dbutils.secrets.get('lsg_finance', 'oce_user') 
              oce_password = dbutils.secrets.get('lsg_finance', 'oce_pass')
              oracle_url = f"jdbc:oracle:thin:username/password@//awsu1-10lhyd03p.amer.thermo.com" + f":1521/HYSA02P_PRIM"
              ##########Redshift Database Connections
              dbutils.widgets.text("redshift_instance","cdwprd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com")
              dbutils.widgets.text("database", "cdwprd")
              gla_user = 'spot_gla_databricks_all'
              gla_password = dbutils.secrets.get('lsg_finance', 'spot_gla_databricks_all_pw')
              rs_username = 'spot_gla_databricks_all'
              rs_password = dbutils.secrets.get('lsg_finance', 'spot_gla_databricks_all_pw')
              stg_user = dbutils.secrets.get('lsg_finance', 'spot_stg_user') 
              stg_password = dbutils.secrets.get('lsg_finance', 'spot_stg_pass')
              url_stg = f"jdbc:redshift://" + getArgument("redshift_instance") + f":5439/"+ getArgument("database") + f"?user=" + stg_user + f"&password=" + stg_password
              url = f"jdbc:redshift://" + getArgument("redshift_instance") + f":5439/"+ getArgument("database") + f"?user=" + gla_user + f"&password=" + gla_password
              temp = "s3a://tfsdl-lsg-spot-prod/" + f"databricks/data"
              delta_path = "s3a://tfsdl-lsg-spot-prod/delta/"
              ##########File path and other variables and params
              timestr=time.strftime('%Y_%m_%d_%H%m')
              dbutils.widgets.text("s3path", "s3a://tfsdl-lsg-spot-test/plnr_landing/")
def run_redshift_sql(sql):
              spotconfig()
              try:
                con = psycopg2.connect(dbname=getArgument("database"), host=getArgument("redshift_instance"), port='5439', user=gla_user, password=gla_password)
                cur = con.cursor()
                cur.execute(sql)
                cur.execute('commit')
                con.commit()
                cur.close()
              except Exception as err:
                raise Exception(str(err))
              

# COMMAND ----------

spotconfig()
