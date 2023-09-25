import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """ Retrieves a list of products from the Yandex Market API.

    Args:
        page (int): The page token to fetch the products from.
        campaign_id (str): The ID of the campaign.
        access_token (str): The access token to authenticate the request.

    Returns:
        dict: The response object containing the list of products.

    Example:
        >>> get_product_list(1, "123456789", "access_token")
        {'result': [{'id': '1', 'name': 'Product 1'}, {'id': '2', 'name': 'Product 2'}, ...]}

    Raises:
        HTTPError: If the API request fails.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """ Updates the stocks of products in the Yandex Market API.

    Args:
        stocks (list): A list of stock objects to update.
        campaign_id (str): The ID of the campaign.
        access_token (str): The access token to authenticate the request.

    Returns:
        dict: The response object containing the updated stocks.

    Raises:
        HTTPError: If the API request fails.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """ Updates the prices of products.

    Args:
        prices (list): A list of prices to be updated.
        campaign_id (str): The ID of the campaign.
        access_token (str): The access token to authenticate the request.

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
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """ Retrieves the offer IDs of the products from the Yandex Market.

    Args:
        campaign_id (str): The ID of the campaign.
        market_token (str): The token for authentication.

    Returns:
        list: A list of offer IDs.

    Example:
        >>> get_offer_ids("123456789", "market_token")
        ["offer_id_1", "offer_id_2", ...]
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """ Creates stock objects for the given watch remnants and offer IDs.

    Args:
        watch_remnants (list): A list of watch remnants.
        offer_ids (list): A list of offer IDs.
        warehouse_id (str): The ID of the warehouse.

    Returns:
        list: A list of stock objects.

    Example:
        >>> create_stocks(watch_remnants, ["offer_id_1", "offer_id_2"], "warehouse_id")
        [
            {
                "sku": "watch_sku_1",
                "warehouseId": "warehouse_id",
                "items": [
                    {
                        "count": 100,
                        "type": "FIT",
                        "updatedAt": "2022-01-01T00:00:00Z"
                    }
                ]
            },
            {
                "sku": "watch_sku_2",
                "warehouseId": "warehouse_id",
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": "2022-01-01T00:00:00Z"
                    }
                ]
            },
            ...
        ]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Creates price objects for the given watch remnants and offer IDs.

    Args:
        watch_remnants (list): A list of watch remnants.
        offer_ids (list): A list of offer IDs.

    Returns:
        list: A list of price objects.

    Example:
        >>> create_prices(watch_remnants, ["offer_id_1", "offer_id_2"])
        [
            {
                "id": "watch_sku_1",
                "price": {
                    "value": 1000,
                    "currencyId": "RUR"
                }
            },
            {
                "id": "watch_sku_2",
                "price": {
                    "value": 2000,
                    "currencyId": "RUR"
                }
            },
            ...
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """ Uploads prices for the given watch remnants to the Yandex Market API.

    Args:
        watch_remnants (list): A list of watch remnants.
        campaign_id (str): The ID of the campaign.
        market_token (str): The token for authentication.

    Returns:
        list: A list of price objects that were uploaded.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """ Uploads stocks for the given watch remnants to the Yandex Market API.

    Args:
        watch_remnants (list): A list of watch remnants.
        campaign_id (str): The ID of the campaign.
        market_token (str): The token for authentication.
        warehouse_id (str): The ID of the warehouse.

    Returns:
        tuple: A tuple containing two lists - the not empty stocks and all stocks.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
