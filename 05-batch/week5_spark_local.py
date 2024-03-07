cd $SPARK_HOME

./sbin/start-master.sh

SPARK_MASTER_URL = "spark://data-platform01-u:7077"

./sbin/start-worker.sh spark://data-platform01-u:7077


