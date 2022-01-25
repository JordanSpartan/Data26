import boto3
import pandas as pd
import io


class S3:

    def __init__(self, bucket_name):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.bucket_name = bucket_name
        self.bucket = self.s3_resource.Bucket(bucket_name)

    # read all csv with the prefix and add them to and return a list of dfs
    def read_all_csv(self):
        prefix_objs = self.bucket.objects.filter(Prefix="python/fish-market")
        df_lst = []
        for obj in prefix_objs:
            try:
                body = obj.get()['Body'].read()
                temp = pd.read_csv(io.BytesIO(body), header=[0], encoding='utf8', sep=',')
                df_lst.append(temp)
            except:
                continue
        return df_lst

    # merge all df in the list of dfs into one
    def merge_dfs(self, df_list):
        merged = pd.concat(df_list, ignore_index=True)
        return merged

    # group and avg the df by species, returning the resulting df
    def group_and_avg(self, df):
        grouped = df.groupby(["Species"]).mean()
        return grouped

    # round all the values in the df to 2 d.p
    def round_all(self, df):
        rounded = df.round(2)
        return rounded

    # convert the df to csv then upload to s3 bucket
    def df_to_s3(self, df):
        df.to_csv("JordanB.csv")
        self.s3_client.upload_file(Filename="JordanB.csv", Bucket=self.bucket_name, Key="Data26/fish/JordanB.csv")


