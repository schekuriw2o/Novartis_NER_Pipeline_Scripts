#!/usr/bin/env python
# coding: utf-8

# In[8]:


import os
import sys
from google.cloud import storage
import sparknlp
from sparknlp.base import *
from sparknlp.common import *
from sparknlp.annotator import *
from sparknlp.training import *
from pyspark.ml import Pipeline
import sparknlp
from sparknlp.base import *
from sparknlp.common import *
from sparknlp.annotator import *
from sparknlp.training import *
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from hashlib import sha256

# from common.utils.gcp_functions import GCPFunctions

gcs_source_filepath = sys.argv[1]
gcs_sentiment_output_filepath = sys.argv[2]
# secret_project = sys.argv[3]
datastore_project = sys.argv[3]
datastore_bucket = sys.argv[4]
'''
nlp_license_secret_manager_key_id = sys.argv[5]
nlp_key_id_secret_manager_key_id = sys.argv[5]
nlp_secret_key_secret_manager_key_id = sys.argv[5]
'''
SENTIMENT_MODEL_NAME = sys.argv[5]

# gcp_client = GCPFunctions.create_gcp_client()


'''
os.environ['SPARK_NLP_LICENSE'] = GCPFunctions.get_secret_manager_values(gcp_client, GCPFunctions.create_secret_name(secret_project,nlp_license_secret_manager_key_id ))
os.environ['AWS_ACCESS_KEY_ID']= GCPFunctions.get_secret_manager_values(gcp_client, GCPFunctions.create_secret_name(secret_project, nlp_key_id_secret_manager_key_id ))
os.environ['AWS_SECRET_ACCESS_KEY'] = GCPFunctions.get_secret_manager_values(gcp_client, GCPFunctions.create_secret_name(secret_project, nlp_secret_key_secret_manager_key_id ))
'''


def start(secret):
    builder = SparkSession.builder.appName("Spark NLP Licensed").master("yarn").config("spark.serializer",
                                                                                       "org.apache.spark.serializer.KryoSerializer").config(
        "spark.kryoserializer.buffer.max", "2000M").config("spark.jars.packages",
                                                           "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.0").config(
        "spark.jars", "https://pypi.johnsnowlabs.com/" + secret + "/spark-nlp-jsl-3.2.3.jar")

    return builder.getOrCreate()


# In[11]:


def sentiment_emotional_analysis(model_name, spark, spark_df):
    documentAssembler = DocumentAssembler().setInputCol("Contents").setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en").setInputCols(["document"]).setOutputCol(
        "sentence_embeddings")

    sentimentdl = SentimentDLModel.pretrained(name=model_name, lang="en").setInputCols(
        ["sentence_embeddings"]).setOutputCol("sentiment")
    nlpPipeline = Pipeline(
        stages=[
            documentAssembler,
            use,
            sentimentdl
        ])

    empty_df = spark.createDataFrame([['']]).toDF("Categories")

    pipelineModel = nlpPipeline.fit(empty_df)
    # df = spark.createDataFrame(pd.DataFrame({"text":text_list}))
    sm_result = pipelineModel.transform(spark_df)

    sentiment_result = sm_result.select(
        F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")).select(
        F.expr("cols['0']").alias("document"), F.expr("cols['1']").alias("sentiment"))

    filtered_df = sentiment_result.filter("document != ''")

    print(gcs_sentiment_output_filepath)
    filtered_df.write.option("sep", "|").mode('overwrite').csv(
        "gs://" + datastore_bucket + "/" + gcs_sentiment_output_filepath, header=False)

    # storage_client = storage.Client(project = datastore_project)

    # bucket_obj = storage_client.get_bucket(datastore_bucket)
    # blob = bucket_obj.blob(gcs_sentiment_output_filepath)
    # blob.upload_from_filename(sentiment_result.write.option("sep","|").mode('append').csv('./sentiment_result',header=False))
    # sentiment_result.write.option("sep","|").mode('overwrite').csv('./sentiment_result', header=False)

    '''
    transfer_files = [f for f in os.listdir('./sentiment_result') if f.endswith('.csv')]  ## Provide path to JSON 
    print(transfer_files)
    for file in transfer_files:
        blob.upload_from_filename(file)
    '''


# In[12]:


# In[13]:


yesterday = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
start_time = yesterday + 'T00:00:00'
end_time = yesterday + 'T23:59:59'
file_name = sha256('{0}_{1}'.format(start_time, end_time).encode('utf-8')).hexdigest()

# In[14]:


non_hc_spark = sparknlp.start()
print(gcs_source_filepath)
f_non_hc_spark_df = non_hc_spark.read.option('sep', '|').csv(gcs_source_filepath, header=True).withColumnRenamed(
    'Sentiment', 'orginal_sentiment')
non_hc_spark_df = f_non_hc_spark_df.withColumn('Contents', F.regexp_replace("Contents", '"', ''))

# In[15]:


# SENTIMENT_MODEL_NAME = 'sentimentdl_use_twitter'
sentiment_emotional_analysis(SENTIMENT_MODEL_NAME, non_hc_spark, non_hc_spark_df)
