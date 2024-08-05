workpath=$1

cd $workpath
source venv_pyetl/bin/activate

cd $workpath/core/etl
/usr/bin/spark-submit --master local[4] \
    --conf spark.pyspark.python=/home/appuser/python_jobs/pyetl/venv_pyetl/bin/python \
    --conf spark.pyspark.driver.python=/home/appuser/python_jobs/pyetl/venv_pyetl/bin/python \
    --conf spark.driver.memory=4g \
    --conf spark.executor.memory=8g \
    --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ', ',') \
    structured_streaming_czdf_gc.py
