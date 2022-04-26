#!/usr/bin/env python
# coding: utf-8

# In[8]:

import os
import sys
#from common.utils.gcp_functions import GCPFunctions
from google.cloud import secretmanager



data_source_project = sys.argv[1]
data_source_bucket = sys.argv[2]
gcs_source_filepath = sys.argv[3]
#gcs_disease_output_filepath = sys.argv[4]
#gcs_clinical_output_filepath = sys.argv[5]
#gcs_psology_output_filepath = sys.argv[6]

nlp_license_secret_manager_key_id = sys.argv[4]
nlp_key_id_secret_manager_key_id= sys.argv[5]
nlp_secret_key_secret_manager_key_id = sys.argv[6]
nlp_secret_key_secret_manager_key = sys.argv[7]x

DISEASES_MODEL_NAME = sys.argv[8]
PSOLOGY_MODEL_NAME = sys.argv[9]
CLINICAL_MODEL_NAME = sys.argv[10]
batch_date = sys.argv[11]
file_name = sys.argv[12]
secret_project = sys.argv[13]

def create_secret_name(project, secret_label):
    "return secret label path"
    return f"projects/{project}/secrets/{secret_label}/versions/latest"

def get_secret_manager_values(gcp_client, name):
    "returns secret value"
    return gcp_client.access_secret_version(name=name).payload.data.decode("UTF-8")

gcp_client = secretmanager.SecretManagerServiceClient()
os.environ['SPARK_NLP_LICENSE'] = get_secret_manager_values(gcp_client, create_secret_name(secret_project,nlp_license_secret_manager_key_id ))
os.environ['AWS_ACCESS_KEY_ID'] = get_secret_manager_values(gcp_client, create_secret_name(secret_project, nlp_key_id_secret_manager_key_id ))
os.environ['AWS_SECRET_ACCESS_KEY']  = get_secret_manager_values(gcp_client, create_secret_name(secret_project,nlp_secret_key_secret_manager_key_id ))
os.environ['SPARK_NLP_KEY'] = get_secret_manager_values(gcp_client, create_secret_name(secret_project, nlp_secret_key_secret_manager_key ))


import glob
import sparknlp
from google.cloud import storage
from sparknlp.base import *
from sparknlp.common import *
from sparknlp.annotator import *
from sparknlp.training import *
import sparknlp_jsl
from sparknlp_jsl.base import *
from sparknlp_jsl.annotator import *
from pyspark.ml import Pipeline
import sparknlp
from sparknlp.base import *
from sparknlp.common import *
from sparknlp.annotator import *
from sparknlp.training import *
import pyspark.sql.functions as F
import sparknlp_jsl
from sparknlp_jsl.base import *
from sparknlp_jsl.annotator import *
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from hashlib import sha256

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'application_default_credentials.json'
google_client = storage.Client(data_source_project) ## add parameeter for project
bucket = google_client.get_bucket(data_source_bucket) ## add parameter for bucket


def start(secret):
    builder = SparkSession.builder.appName("Spark NLP Licensed").master("yarn").config("spark.serializer",
                                                                                       "org.apache.spark.serializer.KryoSerializer").config(
        "spark.kryoserializer.buffer.max", "2000M").config("spark.jars.packages",
                                                           "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.0").config(
        "spark.jars", "https://pypi.johnsnowlabs.com/" + secret + "/spark-nlp-jsl-3.2.3.jar")

    return builder.getOrCreate()


def healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, model_name, spark, df,
                        export_file):

    print("Runnning For ", export_file)
    clinical_ner = MedicalNerModel.pretrained(model_name, "en", "clinical/models").setInputCols(
        ["sentence", "token", "embeddings"]).setOutputCol("ner")

    ner_converter = NerConverter().setInputCols(['sentence', 'token', 'ner']).setOutputCol('ner_chunk')

    nlp_pipeline = Pipeline(stages=[
        document_assembler,
        sentence_detector,
        tokenizer,
        word_embeddings,
        clinical_ner,
        ner_converter])

    empty_df = spark.createDataFrame([['']]).toDF('Contents')
    pipeline_model = nlp_pipeline.fit(empty_df)
    result = pipeline_model.transform(df)

    exploded = F.explode(F.arrays_zip('ner_chunk.result', 'ner_chunk.metadata'))
    select_expression_0 = F.expr("cols['0']").alias("chunk")
    select_expression_1 = F.expr("cols['1']['entity']").alias("ner_label")
    transformed_df = result.select('IDs', 'Contents', exploded.alias("cols")).select('IDs', 'Contents',
                                                          select_expression_0,select_expression_1)
    today_date = datetime.today().strftime('%Y%m%d')

    transformed_df.write.option("sep", "|").mode('overwrite').csv('{}'.format(today_date + model_name), header=True)

    rel_paths = glob.glob(today_date + model_name + '/**', recursive=True)
    for local_file in rel_paths:
        remote_path = f'client_work/{export_file}/{batch_date}/{"/".join(local_file.split(os.sep)[1:])}'
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)


#yesterday = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
#start_time = yesterday + 'T00:00:00'
#end_time = yesterday + 'T23:59:59'
#file_name = sha256('{0}_{1}'.format(start_time, end_time).encode('utf-8')).hexdigest()

healthcare_spark = sparknlp_jsl.start(os.environ['SPARK_NLP_KEY'])
print("input file path is -- > ",gcs_source_filepath + "/" + file_name)
dl_blob = bucket.blob(gcs_source_filepath + "/" + file_name + ".csv")
dl_blob.download_to_filename('source.csv')
f_healthcare_df = healthcare_spark.read.option('sep','|').option("header", True).csv('source.csv', )
healthcare_df = f_healthcare_df.withColumn('Contents', F.regexp_replace("Contents",'"',''))

document_assembler = DocumentAssembler().setInputCol('Contents').setOutputCol('document')

sentence_detector = SentenceDetector().setInputCols(['document']).setOutputCol('sentence')

tokenizer = Tokenizer().setInputCols(['sentence']).setOutputCol('token')

word_embeddings = WordEmbeddingsModel.pretrained('embeddings_clinical', 'en', 'clinical/models').setInputCols(
    ['sentence', 'token']).setOutputCol('embeddings')


#DISEASES_MODEL_NAME = "ner_diseases"
#DISEASES_CSV_FILE = "disease_ner_result"
DISEASES_CSV_FILE = sys.argv[14]
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, DISEASES_MODEL_NAME,
                    healthcare_spark, healthcare_df, DISEASES_CSV_FILE)

#PSOLOGY_MODEL_NAME = "ner_posology"
#PSOLOGY_CSV_FILE = "psology_ner_result"
PSOLOGY_CSV_FILE = sys.argv[15]
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, PSOLOGY_MODEL_NAME,
                    healthcare_spark, healthcare_df, PSOLOGY_CSV_FILE)


#CLINICAL_MODEL_NAME = "ner_clinical"
#CLINICAL_CSV_FILE = "clinical_ner_result"
CLINICAL_CSV_FILE = sys.argv[16]
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, CLINICAL_MODEL_NAME,
                    healthcare_spark, healthcare_df, CLINICAL_CSV_FILE)






