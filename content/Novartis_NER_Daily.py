#!/usr/bin/env python
# coding: utf-8

# In[8]:

import os

os.environ[
    'SPARK_NLP_LICENSE'] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE2NzI0OTUxMzAsImlhdCI6MTY0MDYxMzUzMCwidW5pcXVlX2lkIjoiMWU0MWFmNTYtNjcxZC0xMWVjLTk4MWEtZWEwNWYwOTE5YWFiIiwic2NvcGUiOlsiaGVhbHRoY2FyZTppbmZlcmVuY2UiLCJoZWFsdGhjYXJlOnRyYWluaW5nIl0sInBsYXRmb3JtIjp7Im5hbWUiOiJGbG9hdGluZyIsInRhYmxlX25hbWUiOiJMaWNlbnNlTG9ja3MiLCJ0eXBlIjoibm9kZSJ9LCJhd3MiOnsiQVdTX0FDQ0VTU19LRVlfSUQiOiJBS0lBU1JXU0RLQkdHTE1XUjJJSiIsIkFXU19TRUNSRVRfQUNDRVNTX0tFWSI6IjQyVkJCY0piM0RHZkYrL2U3c2hhdFFOMWVDdytSejAxdWdlWWVBVi8ifX0.WvuIXkAFZ3f-VwHkQ4WfPbYdRDm_Xo1Ze-EaLzXmXq3UXxEhdcVDnbP3Zfdg-_D_9vP9WB1CYoyXr5VM6Q7cq3xUkrC3sHvuoaoBED5djjtbtJcL73icvpaoFq4pgkOpzNt8wNAxkRz8HUe_-fe6Fo-7ZD09j8MB_DHNIgrd4zSrYOwowNG6QR9GQu0l8wZcDPdkMl6wYP5atsH7dF5YdliTHgid9v_xM-PYit4yCX8uRDuXBC3ZNvdGz4mM6dtiL-m2ip_DgD4Lc2Xbk1ka0EONZZONlYX7jRyTIAUdvy_IVUuaUc4Dfc4JmFvz1Ic7fIEjEo-G-_HCOBcMJ4ZHcw"
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIASRWSDKBGGLMWR2IJ'
os.environ['AWS_SECRET_ACCESS_KEY'] = "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"

# In[9]:

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

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'application_default_credentials.json'
google_client = storage.Client('w2odataengineering')
bucket = google_client.get_bucket('w2odataengineering')


def start(secret):
    builder = SparkSession.builder.appName("Spark NLP Licensed").master("yarn").config("spark.serializer",
                                                                                       "org.apache.spark.serializer.KryoSerializer").config(
        "spark.kryoserializer.buffer.max", "2000M").config("spark.jars.packages",
                                                           "com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.0").config(
        "spark.jars", "https://pypi.johnsnowlabs.com/" + secret + "/spark-nlp-jsl-3.2.3.jar")

    return builder.getOrCreate()


def healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, model_name, spark, df,
                        export_file):
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
    transformed_df.write.option("sep", "|").mode('overwrite').csv('{}'.format(today_date+model_name), header=True)
    # ul_blob = bucket.blob('client_work/{}'.format(export_file))
    # ul_blob.upload_from_filename('{}'.format(export_file))
    rel_paths = glob.glob(today_date + model_name + '/**', recursive=True)
    for local_file in rel_paths:
        remote_path = f'client_work/{export_file}/{today_date}/{"/".join(local_file.split(os.sep)[1:])}'
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)


yesterday = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
start_time = yesterday + 'T00:00:00'
end_time = yesterday + 'T23:59:59'
file_name = sha256('{0}_{1}'.format(start_time, end_time).encode('utf-8')).hexdigest()

healthcare_spark = sparknlp_jsl.start("3.3.4-3ae5966fd16758f401475f3fe1faf5ecb5c59365")
dl_blob = bucket.blob('client_work/source/{}.csv'.format(file_name))
dl_blob.download_to_filename('source.csv')
healthcare_df = healthcare_spark.read.option("header", True).csv('source.csv', )

document_assembler = DocumentAssembler().setInputCol('Contents').setOutputCol('document')

sentence_detector = SentenceDetector().setInputCols(['document']).setOutputCol('sentence')

tokenizer = Tokenizer().setInputCols(['sentence']).setOutputCol('token')

word_embeddings = WordEmbeddingsModel.pretrained('embeddings_clinical', 'en', 'clinical/models').setInputCols(
    ['sentence', 'token']).setOutputCol('embeddings')


DISEASES_MODEL_NAME = "ner_diseases"
DISEASES_CSV_FILE = "disease_ner_result"
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, DISEASES_MODEL_NAME,
                    healthcare_spark, healthcare_df, DISEASES_CSV_FILE)

PSOLOGY_MODEL_NAME = "ner_posology"
PSOLOGY_CSV_FILE = "psology_ner_result"
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, PSOLOGY_MODEL_NAME,
                    healthcare_spark, healthcare_df, PSOLOGY_CSV_FILE)


CLINICAL_MODEL_NAME = "ner_clinical"
CLINICAL_CSV_FILE = "clinical_ner_result"
healthcare_analysis(document_assembler, sentence_detector, tokenizer, word_embeddings, CLINICAL_MODEL_NAME,
                    healthcare_spark, healthcare_df, CLINICAL_CSV_FILE)






