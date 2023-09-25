import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """ Retrieves a list of products from the OZON store.

    Args:
        last_id (str): The last ID of the product.
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        list: A list of products.

    Raises:
        HTTPError: If the API call fails.

    Example:
        >>> get_product_list("123", "client123", "token123")
        [
            {
                "product_id": "123",
                "product_name": "Product 1"
            },
            {
                "product_id": "456",
                "product_name": "Product 2"
            },
            ...
        ]

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """ Retrieves the offer IDs of the products from the OZON store.

    Args:
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        list: A list of offer IDs.

    Example:
        >>> get_offer_ids("client123", "token123")
        ["offer123", "offer456", ...]

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """ Updates the prices of products.

    Args:
        prices (list): A list of prices to be updated.
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        dict: A dictionary containing the response from the API call.

    Raises:
        HTTPError: If the API call fails.

    Example:
        >>> update_price([{"product_id": "123", "price": 10.99}], "client123", "token123")
        {
            "status": "success",
            "message": "Prices updated successfully"
        }

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """ Updates the stocks of products.

    Args:
        stocks (list): A list of stocks to be updated.
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        dict: A dictionary containing the response from the API call.

    Raises:
        HTTPError: If the API call fails.

    Example:
        >>> update_stocks([{"product_id": "123", "stock": 100}], "client123", "token123")
        {
            "status": "success",
            "message": "Stocks updated successfully"
        }

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """ Downloads the stock file from the Casio website.

    Returns:
        list: A list of watch remnants.

    Example:
        >>> download_stock()
        [
            {
                "Product Name": "Watch 1",
                "Stock": 10
            },
            {
                "Product Name": "Watch 2",
                "Stock": 5
            },
            ...
        ]

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """ Creates a list of stocks based on watch remnants and offer IDs.

    Args:
        watch_remnants (list): A list of watch remnants.
        offer_ids (list): A list of offer IDs.

    Returns:
        list: A list of stocks.

    Example:
        >>> create_stocks(watch_remnants, ["offer123", "offer456"])
        [
            {
                "offer_id": "offer123",
                "stock": 10
            },
            {
                "offer_id": "offer456",
                "stock": 0
            },
            ...
        ]

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Creates a list of prices based on watch remnants and offer IDs.

    Args:
        watch_remnants (list): A list of watch remnants.
        offer_ids (list): A list of offer IDs.

    Returns:
        list: A list of prices.

    Example:
        >>> create_prices(watch_remnants, ["offer123", "offer456"])
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "offer123",
                "old_price": "0",
                "price": 10.99
            },
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "offer456",
                "old_price": "0",
                "price": 5.99
            },
            ...
        ]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """ Converts the price to a simplified format.

    Args:
        price (str): The price to be converted.

    Returns:
        str: The converted price.

    Example:
        >>> price_conversion("5'990.00 руб.")
        "5990"

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """ Divides a list into parts of size n.

    Args:
        lst (list): The list to be divided.
        n (int): The size of each part.

    Yields:
        list: A part of the original list.

    Example:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """ Uploads prices to the OZON store.

    Args:
        watch_remnants (list): A list of watch remnants.
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        list: A list of prices.

    Example:
        >>> await upload_prices(watch_remnants, "client123", "token123")
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "offer123",
                "old_price": "0",
                "price": 10.99
            },
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "offer456",
                "old_price": "0",
                "price": 5.99
            },
            ...
        ]

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """ Uploads stocks to the OZON store.

    Args:
        watch_remnants (list): A list of watch remnants.
        client_id (str): The client ID for authentication.
        seller_token (str): The seller token for authentication.

    Returns:
        tuple: A tuple containing two lists - not_empty and stocks.
               not_empty: A list of non-empty stocks.
               stocks: A list of all stocks.

    Example:
        >>> await upload_stocks(watch_remnants, "client123", "token123")
        (
            [
                {
                    "offer_id": "offer123",
                    "stock": 10
                },
                ...
            ],
            [
                {
                    "offer_id": "offer123",
                    "stock": 10
                },
                {
                    "offer_id": "offer456",
                    "stock": 0
                },
                ...
            ]
        )

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
