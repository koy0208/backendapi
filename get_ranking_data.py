import boto3
import requests
import json
import time
import uuid
import os
import json
import numpy as np
import re
from datetime import datetime
from amazon.paapi import AmazonAPI


AMAZON_ACCESS_KEY = os.environ["AMAZON_ACCESS_KEY"]
AMAZON_SECRET_KEY = os.environ["AMAZON_SECRET_KEY"]
AMAZON_ASSOCIATE_ID = os.environ["AMAZON_ASSOCIATE_ID"]
AMAZON_COUNTRY = os.environ["AMAZON_COUNTRY"]
RAKUTEN_APP_ID = os.environ["RAKUTEN_APP_ID"]
RAKUTEN_AFFILIATE_ID = os.environ["RAKUTEN_AFFILIATE_ID"]
RAKUTEN_SEARCH_API_URL = os.environ["RAKUTEN_SEARCH_API_URL"]
RAKUTEN_RANKING_API = os.environ["RAKUTEN_RANKING_API"]


class Ranking_list:
    def __init__(self) -> None:
        self.amazon_api = AmazonAPI(
            AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_ID, AMAZON_COUNTRY
        )
        self.category_dict = {
            "チャイルドシート": {"rakuten": 203056, "amazon": "2490442051"},
            "抱っこ紐": {"rakuten": 566089, "amazon": "345974011"},
            "ベビーカー": {"rakuten": 401151, "amazon": "345931011"},
            "ベビーサークル": {"rakuten": 200840, "amazon": "345899011"},
            "おむつ": {"rakuten": 205197, "amazon": "170329011"},
            "セレモニードレス": {"rakuten": 401128, "amazon": "346058011"},
        }

        # 現在の日付を取得
        # 日付を 'yyyy/mm' の形式でフォーマット
        self.formatted_date = datetime.now().strftime("%Y-%m")

        #  AWS情報
        self.s3 = boto3.client("s3")
        self.bucket_name = "ec-ranking-data"
        self.athena = boto3.client("athena")

    def get_rakuten_result(self, item_json, category):
        point = (
            np.round(int(item_json["itemPrice"]) / 1.1) // 100 * item_json["pointRate"]
        )
        res_json = {
            "item_code": item_json["itemCode"],
            "item_name": item_json["itemName"],
            "item_description": re.sub("^.*＞", "", item_json["itemCaption"]),
            "item_price": int(item_json["itemPrice"]),
            "item_url": item_json["itemUrl"],
            "item_img": item_json["mediumImageUrls"][0],
            "item_point_rate": item_json["pointRate"],
            "item_point": point,
            "ranking": item_json["rank"],
            "shop": "楽天",
            "get_month": self.formatted_date,
            "category": category,
        }
        return res_json

    def get_amazon_result(self, item_json, category, rank=1):
        item_feature = item_json.item_info.features
        description = "" if item_feature is None else item_feature.display_values
        res_json = {
            "item_code": item_json.asin,
            "item_name": item_json.item_info.title.display_value,
            "item_description": description,
            "item_price": item_json.offers.listings[0].price.amount,
            "item_url": item_json.detail_page_url,
            "item_img": item_json.images.primary.large.url,
            "item_point_rate": 0,
            "item_point": 0,
            "ranking": rank,
            "shop": "amazon",
            "get_month": self.formatted_date,
            "category": category,
        }
        return res_json

    # カテゴリーランキングを取得するAPI
    def get_rakuten_category_ranking(self, category, page=1):
        params = {
            "applicationId": RAKUTEN_APP_ID,
            "formatVersion": 2,
            "genreId": self.category_dict[category]["rakuten"],
            "affiliateId": RAKUTEN_AFFILIATE_ID,
            "format": "json",
            "page": page,
        }
        response_rakuten = requests.get(RAKUTEN_RANKING_API, params=params).json()
        if response_rakuten.get("error"):
            item_json_list = []
        else:
            item_json_list = [
                self.get_rakuten_result(r, category) for r in response_rakuten["Items"]
            ]

        return item_json_list

    # カテゴリーランキングを取得するAPI
    # amazonにはないので、カテゴリー名で検索する
    def get_amazon_category_ranking(self, category, page=1):
        try:
            res_amazon = self.amazon_api.search_items(
                keywords=category,
                browse_node_id=self.category_dict[category]["amazon"],
                sort_by="AvgCustomerReviews",
                item_page=page,
            )

            item_json_list = [
                self.get_amazon_result(j, category, rank=(i + 1) + ((page - 1) * 10))
                for i, j in enumerate(res_amazon["data"])
            ]
        except:
            item_json_list = []

        return item_json_list

    def upload_ranking_data(self, category):
        ranking_list = []
        # for page in range(1, 34):
        for page in range(1, 3):
            print(page)
            rankuten_item_json_list = self.get_rakuten_category_ranking(category, page)
            page_max = page * 3
            for a_page in range(page_max - 2, page_max + 1):
                print(a_page)
                amazon_item_json_list = self.get_amazon_category_ranking(
                    category, a_page
                )
                ranking_list += amazon_item_json_list
            if not len(rankuten_item_json_list):
                break
            ranking_list += rankuten_item_json_list

        # S3にデータを格納
        for rl in ranking_list:
            key = f"ranking_data/category={rl['category']}/{rl['item_code']}_{rl['get_month']}.json"
            ranking_json = json.dumps(rl)
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=ranking_json)

    def create_table(self):
        # Athenaにテーブルを作成
        database = "easy_joy"
        table = "ec_ranking"
        query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
                item_code string,
                item_name string,
                item_description string,
                item_price double,
                item_url string,
                item_img string,
                item_point_rate double,
                item_point double,
                ranking int,
                shop string,
                get_month string,
                category string
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
            'serialization.format' = '1'
            ) LOCATION 's3://{self.bucket_name}/ranking_data/'
            TBLPROPERTIES ('has_encrypted_data'='false');
        """

        query_id = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket_name}/athena-results/"
            },
        )["QueryExecutionId"]

        # Athenaのクエリが完了するまで待つ
        while True:
            response = self.athena.get_query_execution(QueryExecutionId=query_id)
            if response["QueryExecution"]["Status"]["State"] in [
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
            ]:
                break
            time.sleep(1)


crl = Ranking_list()
crl.upload_ranking_data("チャイルドシート")
crl.create_table()
