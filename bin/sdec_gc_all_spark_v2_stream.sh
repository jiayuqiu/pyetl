workpath=$1
cd $workpath
source $workpath/venv_pyetl/bin/activate

current_date=$(date +%Y-%m-%d)
previous_date=$(date -d "$current_date -1 day" +%Y-%m-%d)
start_time=$(date -d "$previous_date" +%s)
current_date=$(date -d @$start_time +"%Y-%m-%d")

export PYSPARK_DRIVER_PYTHON=`which python`
export PYSPARK_PYTHON=./venv_pyetl/bin/python

/usr/bin/spark-submit \
    --archives hdfs:///user/root/cloud/venv_pyetl.tar#venv_pyetl \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv_pyetl/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./venv_pyetl/bin/python \
    --master yarn \
    --deploy-mode client \
    --num-executors 16 \
    --executor-memory 4G \
    --py-files hdfs:///user/qjy20472/cloud/pyetl/core.zip \
    --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
    core/etl/sdec_spark_gc_v2.py --env=prod --date=${current_date} --data_source=sdec --data_type=gc --mode=stream
