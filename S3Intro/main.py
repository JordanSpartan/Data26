import boto3
import pandas as pd
import io

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket_name = "data-eng-resources"
bucket = s3_resource.Bucket(bucket_name)


# read all csv with the prefix and add them to and return a list of dfs
def read_all_csv():
    prefix_objs = bucket.objects.filter(Prefix="python/fish-market")
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
def merge_dfs(df_list):
    merged = pd.concat(df_list, ignore_index=True)
    return merged


# group and avg the df by species, returning the resulting df
def group_and_avg(df):
    grouped = df.groupby(["Species"]).mean()
    return grouped


# round all the values in the df to 2 d.p
def round_all(df):
    rounded = df.round(2)
    return rounded


# convert the df to csv then upload to s3 bucket
def df_to_s3(df):
    df.to_csv("JordanB.csv")
    s3_client.upload_file(Filename="JordanB.csv", Bucket=bucket_name, Key="Data26/fish/JordanB.csv")


if __name__ == "__main__":

    df = merge_dfs(read_all_csv())
    final = round_all(group_and_avg(df))
    df_to_s3(final)




