from datetime import datetime, timedelta, timezone

import requests

import data_structures
import flask_app


def currency_data_retriever(retrieval_date_from, retrieval_date_to):
    if retrieval_date_from == retrieval_date_to:
        retrieval_date_to = retrieval_date_to + timedelta(1)

    coindesk_url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from={retrieval_date_from.timestamp()}&to={retrieval_date_to.timestamp()}"
    print('Retrieving Currency Information from', coindesk_url)
    currency_request = requests.get(coindesk_url, headers=data_structures.default_headers)
    currency_request.raise_for_status()

    currency_data = currency_request.json()['prices']

    currency_data_datetime = list(map(lambda x: [datetime.fromtimestamp(int(x[0])/1000), x[1]], currency_data))

    date_set = {date[0].date() for date in currency_data_datetime}

    mean_day_values = {}

    for currency_date in date_set:
        day_sum = 0
        day_count = 0
        for y in currency_data_datetime:
            if currency_date == y[0].date():
                day_sum += y[1]
                day_count += 1
        day_mean = day_sum / day_count
        mean_day_values[currency_date.strftime('%Y-%m-%d')] = day_mean

    exchange_rate_data = exchange_data_retrieval(retrieval_date_from, retrieval_date_to)
    consolidated_data = {}
    for working_date, usd_value in mean_day_values.items():
        consolidated_data[working_date] = {
            'USD': usd_value,
        }
        exchange_date = working_date
        while exchange_date not in exchange_rate_data:
            exchange_date_object = datetime.fromisoformat(exchange_date).replace(tzinfo=timezone.utc)
            subtracted_date = (exchange_date_object - timedelta(days=1))
            exchange_date = subtracted_date.strftime('%Y-%m-%d')

        for currency in flask_app.currency_list:
            if currency in exchange_rate_data[exchange_date]:
                consolidated_data[working_date][currency] = round(
                    float(usd_value) * float(exchange_rate_data[exchange_date][currency]), 2)
    return consolidated_data


def exchange_data_retrieval(date_from, date_to):
    from_weekday = date_from.weekday()
    date_from_string = date_from.strftime('%Y-%m-%d')

    if from_weekday > 4:
        date_from = date_from - timedelta(days=from_weekday - 4)
        date_from_string = date_from.strftime('%Y-%m-%d')
        print('Exchange retrieval updated to proceeding Friday')
    try:
        exchange_base_url = "https://api.freecurrencyapi.com/v1/historical"
        exchange_api_key = "hUu0gsa0iSysL0s2dRdiOVsEd30nwgde9tAyWhPB"
        exchange_rate_retrieval_url = f"{exchange_base_url}?base_currency=USD&date_from={date_from_string}&date_to={date_to.strftime('%Y-%m-%d')}&apikey={exchange_api_key}&currency={flask_app.currency_list}"

        print(f"Retrieving Exchange Rate Data from {exchange_rate_retrieval_url}")
        exchange_headers = data_structures.default_headers
        exchange_headers['apikey'] = exchange_api_key
        exchange_request = requests.get(url=exchange_rate_retrieval_url, headers=exchange_headers, timeout=15)
        exchange_request.raise_for_status()
        exchange_rate_data = exchange_request.json()['data']

    except requests.exceptions.HTTPError as ex:
        if ex.response.status_code == 500:
            date_from = date_from - timedelta(days=1)
            date_from_string = date_from.strftime('%Y-%m-%d')
            return exchange_data_retrieval(date_from_string, date_to.strftime('%Y-%m-%d'))
        if not ex.response.status_code == 429:
            print('Other HTTP error occurred', ex)
    except requests.exceptions.ReadTimeout as timeout:
        print("Request read timeout", timeout)
    except requests.exceptions.ConnectionError as connection_error:
        print("Connection Error Occurred", connection_error)
    else:
        return exchange_rate_data
