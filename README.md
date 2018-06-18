# Simple little Spark application

For run:

```
$ spark-submit \
--packages com.databricks:spark-csv_2.11:1.5.0 \
--class xt84.info.spark.sample.little.Main </path/to/app.jar> \
--dep </path/to/dep.txt \
--emp </path/to/emp.txt> \
--output </path/to/output/dir>
```