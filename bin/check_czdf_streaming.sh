if ps aux | grep -v grep | grep structured_streaming_czdf_gc.py > /dev/null
then
    echo "Process is running."
else
    echo "Process is not running. Starting process..."
    bash /home/appuser/python_jobs/pyetl/bin/czdf_streaming.sh /home/appuser/python_jobs/pyetl > /home/appuser/python_jobs/pyetl/czdf_streaming.log &
fi
