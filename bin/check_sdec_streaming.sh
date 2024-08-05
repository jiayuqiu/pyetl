# sdec_spark_gc_v2.py, --mode=stream, 这两个字符串同时存在则说明sdec etl消费程序正常
if ps aux | grep -v grep | grep "sdec_spark_gc_v2.py.*--mode=stream" > /dev/null
then
    echo "Process is running."
else
    echo "Process is not running. Starting process..."
    bash /home/appuser/python_jobs/pyetl/bin/sdec_gc_all_spark_v2_stream.sh /home/appuser/python_jobs/pyetl > /home/appuser/python_jobs/pyetl/log/sdec_streaming.log &
fi
