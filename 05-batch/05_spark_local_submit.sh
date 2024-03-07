MASTER_URL="spark://data-platform01-u:7077"

spark-submit \
	--master="${MASTER_URL}" \
	--executor-memory 4G \
	--driver-memory 4G \
	week5_homework.py \
		--input_loc=data/raw/fhv/2019/10/fhv_tripdata_2019_10.csv \
		--output_loc=fhv/2019/10/
