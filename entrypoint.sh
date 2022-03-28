#!/bin/bash
export_json () {
    for s in $(echo $values | jq -r 'to_entries|map("\(.key)=\(.value|tostring)")|.[]' $1 ); do
        export $s
    done
}

export_json "/content/spark_nlp_for_healthcare.json"

pip install --upgrade spark-nlp-jsl==3.3.4 --user --extra-index-url https://pypi.johnsnowlabs.com/$SECRET

if [ $? != 0 ];
then
    exit 1
fi

pip install --upgrade spark-nlp-display

python3 /content/Novartis_NER_Daily.py --server.port=8501
pip install pyspark==3.2.0 spark-nlp