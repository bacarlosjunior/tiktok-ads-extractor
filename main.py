import base64
import os
from numpy import append
import pandas
import requests
import json
from datetime import timedelta
from google.cloud.exceptions import NotFound
from tiktok_to_bq import exist_dataset_table, insert_df_bq
from tiktok_to_bq import client, schema, clustering_fields_tiktok
from pandas import Timestamp
import datetime
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa


def read_json(file_json):
    with open(file_json, 'r', encoding='utf8') as f:
        return json.load(f)

today = datetime.datetime.now()
yesterday = (today - timedelta(1))

def interval_day(choice):
    if choice == 'yesterday':
        return yesterday.strftime("%Y-%m-%d")
    elif choice == 'past_30':
        past_30 = (today - timedelta(30))
        return past_30.strftime("%Y-%m-%d")

PATH = "/open_api/v1.2/reports/integrated/get/"

def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get_campaign_dateails(json_str,access_token):

    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": access_token,
    }
    response = requests.get(url, headers=headers)
    response = response.json()
    
    campaign_data_df = pandas.DataFrame(columns=["date","ad_name","ad_id","campaign_name","campaign_id",
    "adgroup_name","adgroup_id","spend","impressions","conversion","conversion_rate","cpc",
    "cpm","clicks","ctr","cost_per_conversion","real_time_conversion",
    "real_time_cost_per_conversion","real_time_conversion_rate","result","cost_per_result",
    "result_rate","real_time_result","real_time_cost_per_result","real_time_result_rate","country"])

    campaigns = response["data"]["list"]

    for campaign in campaigns:
        if "metrics" in campaign:
            tmp_dict = {}
            try:
                spend = campaign["metrics"]["spend"]
            except:
                spend = None
            tmp_dict["spend"] = float(spend)

            try:
                ad_name = campaign["metrics"]["ad_name"]
            except:
                ad_name = None
            tmp_dict["ad_name"] = ad_name

            try:
                campaign_name = campaign["metrics"]["campaign_name"]
            except:
                campaign_name = None
            tmp_dict["campaign_name"] = campaign_name

            try:
                campaign_id = campaign["metrics"]["campaign_id"]
            except:
                campaign_id = None
            tmp_dict["campaign_id"] = campaign_id

            try:
                adgroup_id = campaign["metrics"]["adgroup_id"]
            except:
                adgroup_id = None
            tmp_dict["adgroup_id"] = adgroup_id

            try:
                adgroup_name = campaign["metrics"]["adgroup_name"]
            except:
                adgroup_name = None
            tmp_dict["adgroup_name"] = adgroup_name

            try:
                impressions = campaign["metrics"]["impressions"]
            except:
                impressions = None
            tmp_dict["impressions"] = int(impressions)

            try:
                conversion = campaign["metrics"]["conversion"]
            except:
                conversion = None
            tmp_dict["conversion"] = int(conversion)

            try:
                conversion_rate = campaign["metrics"]["conversion_rate"]
            except:
                conversion_rate = None
            tmp_dict["conversion_rate"] = float(conversion_rate)

            try:
                cpc = campaign["metrics"]["cpc"]
            except:
                cpc = None
            tmp_dict["cpc"] = float(cpc)

            try:
                cpm = campaign["metrics"]["cpm"]
            except:
                cpm = None
            tmp_dict["cpm"] = float(cpm)

            try:
                clicks = campaign["metrics"]["clicks"]
            except:
                clicks = None
            tmp_dict["clicks"] = int(clicks)
            
            try:
                ctr = campaign["metrics"]["ctr"]
            except:
                ctr = None
            tmp_dict["ctr"] = float(ctr)

            try:
                cost_per_conversion = campaign["metrics"]["cost_per_conversion"]
            except:
                cost_per_conversion = None
            tmp_dict["cost_per_conversion"] = float(cost_per_conversion)

            try:
                real_time_conversion = campaign["metrics"]["real_time_conversion"]
            except:
                real_time_conversion = None
            tmp_dict["real_time_conversion"] = float(real_time_conversion)

            try:
                real_time_cost_per_conversion = campaign["metrics"]["real_time_cost_per_conversion"]
            except:
                real_time_cost_per_conversion = None
            tmp_dict["real_time_cost_per_conversion"] = float(real_time_cost_per_conversion)

            try:
                real_time_conversion_rate = campaign["metrics"]["real_time_conversion_rate"]
            except:
                real_time_conversion_rate = None
            tmp_dict["real_time_conversion_rate"] = float(real_time_conversion_rate)

            try:
                result = campaign["metrics"]["result"]
            except:
                result = None
            tmp_dict["result"] = int(result)

            try:
                cost_per_result = campaign["metrics"]["cost_per_result"]
            except:
                cost_per_result = None
            tmp_dict["cost_per_result"] = float(cost_per_result)

            try:
                result_rate = campaign["metrics"]["result_rate"]
            except:
                result_rate = None
            tmp_dict["result_rate"] = float(result_rate)

            try:
                real_time_result = campaign["metrics"]["real_time_result"]
            except:
                real_time_result = None
            tmp_dict["real_time_result"] = int(real_time_result)

            try:
                real_time_cost_per_result = campaign["metrics"]["real_time_cost_per_result"]
            except:
                real_time_cost_per_result = None
            tmp_dict["real_time_cost_per_result"] = float(real_time_cost_per_result)

            try:
                real_time_result_rate = campaign["metrics"]["real_time_result_rate"]
            except:
                real_time_result_rate = None
            tmp_dict["real_time_result_rate"] = float(real_time_result_rate)
        
        if "dimensions" in campaign:
            
            try:
                ad_id = campaign["dimensions"]["ad_id"]
            except:
                ad_id = None
            tmp_dict["ad_id"] = ad_id

            try:
                date = campaign["dimensions"]["stat_time_day"]
            except:
                date = None
            #tmp_dict["date"] = date
            
            date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            tmp_dict["date"] = date_obj.date()

            try:
                country = campaign["dimensions"]["country_code"]
            except:
                country = None
            tmp_dict["country"] = country

        df_tmp_dict = pandas.DataFrame(pandas.Series(tmp_dict)).T
        campaign_data_df = pandas.concat([campaign_data_df, df_tmp_dict])
    
    return campaign_data_df


def main_tiktok(event,context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    credentials = read_json("tiktok_cred.json")

    if pubsub_message == 'get_tiktok':

        metrics_list = ["spend","ad_name","campaign_name","campaign_id","adgroup_name",
        "adgroup_id","impressions","conversion","conversion_rate","cpc",
        "cpm","clicks","ctr","cost_per_conversion","real_time_conversion",
        "real_time_cost_per_conversion","real_time_conversion_rate","result","cost_per_result",
        "result_rate","real_time_result","real_time_cost_per_result","real_time_result_rate"]
        metrics = json.dumps(metrics_list)

        data_level = "AUCTION_AD"
        page_size = 10
        service_type = "AUCTION"
        report_type = "AUDIENCE"
        page = 1
        dimensions_list = ["country_id","ad_id","stat_time_day"]
        dimensions = json.dumps(dimensions_list)

        table_id = event['attributes']['table_id']
        dataset_id = event['attributes']['dataset_id']
        project_id = event['attributes']['project_id']
        date_preset = event['attributes']['date_preset']
        advertiser_id = event['attributes']["advertiser_id"]
        access_token = credentials["access_token"]
        start_date = interval_day(date_preset)
        end_date = yesterday.strftime("%Y-%m-%d")


        # Args in JSON format
        my_args = "{\"metrics\": %s, \"data_level\": \"%s\", \"end_date\": \"%s\", \"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"report_type\": \"%s\", \"page\": \"%s\", \"dimensions\": %s}" % (metrics, data_level, end_date, page_size, start_date, advertiser_id, service_type, report_type, page, dimensions)
        campaign_dateails = get_campaign_dateails(my_args,access_token)

        exist_dataset_table(client, table_id, dataset_id, project_id, schema,clustering_fields_tiktok)
        insert_df_bq(schema,client, table_id, dataset_id, project_id, campaign_dateails)
