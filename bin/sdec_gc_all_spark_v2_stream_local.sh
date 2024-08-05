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
    --master local[16] \
    --conf spark.pyspark.python=/home/appuser/python_jobs/pyetl/venv_pyetl/bin/python \
    --conf spark.pyspark.driver.python=/home/appuser/python_jobs/pyetl/venv_pyetl/bin/python \
    --conf spark.driver.memory=4g \
    --executor-memory 8G \
    --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
    core/etl/sdec_spark_gc_v2.py --env=prod --date=${current_date} --data_source=sdec --data_type=gc --mode=stream
