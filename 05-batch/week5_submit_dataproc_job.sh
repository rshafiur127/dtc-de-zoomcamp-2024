gcloud dataproc jobs submit pyspark \
	--cluster=dtc-zoomcamp-2024 \
	--region=us-central1 \
	gs://dtc-de-411905-week5/code/week5_homework.py \
	-- \
		--input_loc=gs://dtc-de-411905-week5/data/raw/fhv/2019/01/fhv_tripdata_2019_01.csv \
		--output_loc=gs://dtc-de-411905-week5/fhv/2019/01/
