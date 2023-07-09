import requests
import numpy as np
import os
import re
import time
import boto3
from fastapi import FastAPI
from mangum import Mangum
from typing import Optional
from amazon.paapi import AmazonAPI
from starlette.middleware.cors import CORSMiddleware  # 追加

app = FastAPI()
# CORSを回避するために追加（今回の肝）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,  # 追記により追加
    allow_methods=["*"],  # 追記により追加
    allow_headers=["*"],  # 追記により追加
)

AMAZON_ACCESS_KEY = os.environ["AMAZON_ACCESS_KEY"]
AMAZON_SECRET_KEY = os.environ["AMAZON_SECRET_KEY"]
AMAZON_ASSOCIATE_ID = os.environ["AMAZON_ASSOCIATE_ID"]
AMAZON_COUNTRY = os.environ["AMAZON_COUNTRY"]
RAKUTEN_APP_ID = os.environ["RAKUTEN_APP_ID"]
RAKUTEN_AFFILIATE_ID = os.environ["RAKUTEN_AFFILIATE_ID"]
RAKUTEN_SEARCH_API_URL = os.environ["RAKUTEN_SEARCH_API_URL"]
RAKUTEN_RANKING_API = os.environ["RAKUTEN_RANKING_API"]

amazon_api = AmazonAPI(
    AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_ID, AMAZON_COUNTRY
)


athena = boto3.client("athena")


def get_rakuten_result(item_json, rank=1):
    point = np.round(int(item_json["itemPrice"]) / 1.1) // 100 * item_json["pointRate"]
    res_json = {
        "item_code": item_json["itemCode"],
        "item_name": item_json["itemName"],
        "item_description": re.sub("^.*＞", "", item_json["itemCaption"]),
        "item_price": int(item_json["itemPrice"]),
        "item_url": item_json["itemUrl"],
        "item_img": item_json["mediumImageUrls"][0],
        "item_point_rate": item_json["pointRate"],
        "item_point": point,
        "ranking": rank,
        "shop": "楽天",
    }
    return res_json


def get_amazon_result(item_json, rank=1):
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
    }
    return res_json


def search_item(
    keyword, category="全て", max_price=100000, min_price=0, sort="standard", page=1
):
    search_category = {
        "全て": {"rakuten": 100533, "amazon": "344845011"},
        "チャイルドシート": {"rakuten": 566088, "amazon": "2490442051"},
        "ファッション": {"rakuten": 111102, "amazon": "345991011"},
        "シューズ": {"rakuten": 200811, "amazon": "2032360051"},
        "ベビーカー": {"rakuten": 200833, "amazon": "345931011"},
        "抱っこ紐": {"rakuten": 566089, "amazon": "345971011"},
        "寝具": {"rakuten": 200822, "amazon": "1910016051"},
        "ベッド": {"rakuten": 200822, "amazon": "1910002051"},
        "インテリア": {"rakuten": 566090, "amazon": "1910002051"},
        "おふろ・バス用品": {"rakuten": 200815, "amazon": "345914011"},
        "おむつ・トイレ": {"rakuten": 213972, "amazon": "345889011"},
        "ミルク・離乳食": {"rakuten": 200827, "amazon": "345977011"},
        "その他": {"rakuten": 100533, "amazon": "344845011"},
    }
    sort_dict = {
        "standard": {"rakuten": "standard", "amazon": "Featured"},
        "min_price": {"rakuten": "+itemPrice", "amazon": "Price:LowToHigh"},
        "max_price": {"rakuten": "-itemPrice", "amazon": "Price:HighToLow"},
        "review": {"rakuten": "-reviewAverage", "amazon": "AvgCustomerReviews"},
    }
    params = {
        "applicationId": RAKUTEN_APP_ID,
        "formatVersion": 2,
        "keyword": keyword,
        "format": "json",
        "genreId": search_category[category]["rakuten"],
        "maxPrice": max_price,
        "minPrice": min_price,
        "sort": sort_dict[sort]["rakuten"],
        "affiliateId": RAKUTEN_AFFILIATE_ID,
        "page": page,
        "carrier": 2,
    }
    res_rakuten = requests.get(RAKUTEN_SEARCH_API_URL, params=params).json()
    res_amazon = amazon_api.search_items(
        keywords=keyword,
        browse_node_id=search_category[category]["amazon"],
        sort_by=sort_dict[sort]["amazon"],
        item_page=page,
    )

    rakuten_items = [get_rakuten_result(j) for j in res_rakuten["Items"]]
    amazon_items = [get_amazon_result(j) for j in res_amazon["data"]]
    items_list = rakuten_items + amazon_items
    sorted_items_list = sorted(items_list, key=lambda x: x["item_price"])
    item_json = {"result": sorted_items_list}

    return item_json


def get_query(query):
    database = "easy_joy"
    query_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://ec-ranking-data/athena-results/"},
    )["QueryExecutionId"]

    # Athenaのクエリが完了するまで待つ
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_id)
        if response["QueryExecution"]["Status"]["State"] in [
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        ]:
            break
        time.sleep(1)

    query_result_paginator = athena.get_paginator("get_query_results")
    query_result_iter = query_result_paginator.paginate(
        QueryExecutionId=query_id, PaginationConfig={"PageSize": 1000}
    )

    user_data = sum(list(map(lambda x: x["ResultSet"]["Rows"], query_result_iter)), [])

    query_list = []
    colmuns = [d["VarCharValue"] for d in user_data[0]["Data"]]
    for ud in user_data[1:]:
        values = [d["VarCharValue"] for d in ud["Data"]]
        query_dict = {k: v for k, v in zip(colmuns, values)}
        query_list.append(query_dict)
    return {
        "statusCode": 200,
        "result": query_list,
    }


def get_category_ranking(get_month, category, max_price=100000, min_price=0):
    database = "easy_joy"
    table = "ec_ranking"
    # Athenaテーブルの情報をPythonから呼び出す
    query = f"""
    SELECT 
        item_code,
        item_name,
        item_description,
        CAST(item_price AS int) AS item_price,
        item_url,
        item_img,
        item_point_rate,
        CAST(item_point AS int) AS item_point,
        shop,
        get_month,
        category,
        CAST(ROW_NUMBER()OVER(PARTITION BY shop ORDER BY ranking) AS int) AS ranking
    FROM {database}.{table} 
    WHERE category = '{category}' AND item_price BETWEEN {min_price} AND {max_price} AND get_month = '{get_month}'
    ORDER BY ranking 
    LIMIT 20
    """

    responce = get_query(query)

    return responce


@app.get("/health")
async def get_health():
    return {"message": "OK !"}


@app.get("/ranking")
async def post_ranking(
    get_month: str,
    category: str,
    min_price: int = None,
    max_price: int = None,
):
    # Noneでないパラメーターだけを抽出
    params = {
        "get_month": get_month,
        "category": category,
        "min_price": min_price,
        "max_price": max_price,
    }
    valid_params = {k: v for k, v in params.items() if v is not None}
    response = get_category_ranking(**valid_params)

    return response


@app.get("/search_item")
async def post_search(
    keyword: str,
    category: str = None,
    max_price: int = None,
    min_price: int = None,
    sort: str = None,
    page: int = None,
):
    params = {
        "keyword": keyword,
        "category": category,
        "max_price": max_price,
        "min_price": min_price,
        "sort": sort,
        "page": page,
    }

    # Noneでないパラメーターだけを抽出
    valid_params = {k: v for k, v in params.items() if v is not None}

    response = search_item(**valid_params)
    return response
