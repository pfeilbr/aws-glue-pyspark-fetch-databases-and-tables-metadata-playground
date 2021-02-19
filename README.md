# aws-glue-pyspark-fetch-databases-and-tables-metadata-playground

example AWS Glue pyspark job script that fetches all the catalog databases and tables metadata.

see [`main.py`](main.py)

* first method uses [spark sql](https://spark.apache.org/sql/)
* second method uses [python boto3 Glue client](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html) to interact with [Glue API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html) directly



## Notes

ensure `--enable-glue-datacatalog` is enabled for glue job to allow spark sql to access metadata catalog

![](https://www.evernote.com/l/AAG3O9zQGjhBQYiqT7_owkUm9K-UXd0bMCEB/image.png)

Glue Console Script View

![](https://www.evernote.com/l/AAG2b5Bdis5KFbt6ijxtySgIG7e2P8jPE0UB/image.png)
