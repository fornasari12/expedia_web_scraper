from google.cloud import bigquery
import pandas as pd


class BigQueryClient:

    def __init__(self, schema=None, table_id=None, df=None):
        self.client = bigquery.Client()
        self.schema = schema
        self.table_id = table_id
        self.df = df

    def create_table(self, exists_ok=True):
        self.client.create_table(bigquery.Table(self.table_id,
                                                schema=self.schema),
                                 exists_ok=exists_ok)

    def upload_dataframe_to_table(self, truncate=False):
        job_config = bigquery.LoadJobConfig(schema=self.schema)
        if truncate:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job = self.client.load_table_from_dataframe(self.df, self.table_id, job_config=job_config)

        job.result()

    def load_dataframe_from_bigquery(self, query):
        bigquery_data = []
        query_job = self.client.query(query)
        print(query_job)
        results = query_job.result()

        for row in results:

            row_data = {
                "hotel": row.hotel,
                "execution_date": row.execution_date,
                "execution_timestamp": row.execution_timestamp,
                "checkin_date": row.checkin_date,
                "checkout_date": row.checkout_date,
                "room_name": row.room_name,
                "price": row.price_option_1,
                        }
            bigquery_data.append(row_data)

        df_1 = pd.DataFrame(bigquery_data)
        return df_1


