

import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from timeit import default_timer as timer

import pandas as pd
import requests
from requests.exceptions import HTTPError


def add_data(route_dict, booking_token_list, json_response, use_date, key):
    """
    Забирает данные о перелете,
    выбирает из данных поля booking_token, price
    добовляет booking_token в список booking_token_list
    добовляет дата + price в словарь route_dict по ключу направления

    :param dict: route_dict
    :param list: booking_token_list
    :param json: json_response
    :param str: use_date
    :param str: key
    :return route_dict, booking_token_list
    """
    route_dict[key].append(
        (use_date, json_response['data'][0]['price']))
    booking_token_list.append(
        ((key, json_response['data'][0]['booking_token'])))
    return route_dict, booking_token_list


def process(query):
    """
    Отправляет запрос к точки API ,
    забирает данные о перелете,
    формирует json


    :param str: query
    :return result
    """

    result = None
    session = requests.Session()

    try:
        response = session.get(query)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        result = response.json()
    return result


def thread_pool(process, query):
    with ProcessPoolExecutor() as executor:
        future = executor.submit(process, query)
        json_response = future.result()

    return json_response

def get_data_from_api():
    """
    Хранит данные, запускает process, add_data

    :return dict, list: route_dict, booking_token_list
    """
    start = timer()

    # parametrs
    route_dict = {
        'ALA-TSE': [],
        'TSE-ALA': [],
        'ALA-MOW': [],
        'MOW-ALA': [],
        'ALA-CIT': [],
        'CIT-ALA': [],
        'TSE-MOW': [],
        'MOW-TSE': [],
        'TSE-LED': [],
        'LED-TSE': []
    }

    booking_token_list = []
    use_date = ''
    amount_of_days = 31
    endpoint_get_data = "https://api.skypicker.com/flights?"
    print('[INFO]: Start create request')

    for key in route_dict.keys():

        lst = key.split('-')
        fly_from = lst[0]
        fly_to = lst[1]

        for day in range(amount_of_days + 1):
            use_date = (date.today() + timedelta(days=day)
                        ).strftime("%d/%m/%Y")
            query = endpoint_get_data + \
                f"fly_from={fly_to}&fly_to={fly_from}&date_from={use_date}" + \
                "&curr=KZT&adults=1&children=0&infants=0&partner=picky&v=3"

            json_response = thread_pool(process, query)

            if len(json_response['data']) == 0:
                continue

            route_dict, booking_token_list = add_data(
                route_dict, booking_token_list, json_response, use_date, key)

    print('[INFO]: API connection. Success!')
    print('[INFO]: Finish create data')
    end = timer()
    print(
        f'\nFunc took {round(end-start, 4)} for exution\n')
    return route_dict, booking_token_list


def create_cache(route_dict):
    """
    Записывает данные в табличном представлении .csv
    сортирует по самому дешевому билету за месяц
    :param dict
    :return: None
    """

    cache = 'cache'

    for value in route_dict.values():
        value.sort(key=lambda tup: tup[1])

    if not os.path.exists(cache):
        os.mkdir(cache)

    csv_file = 'cache/low_price_calendar.csv'

    try:
        df = pd.DataFrame.from_dict(route_dict)
        df.to_csv(csv_file)
    except IOError as er:
        print(f"[ERROR INFO]: {er}")


def check_valid_ticket(route_dict, booking_token_list):
    """
    Отправляет запрос к точки API для проверки
    забирает информацию о билете по токену,
    выбирает из данных поля flights_checked, flights_invalid, price_change
    проверяет на валидность, подтверждение, изменение цены билета
    обновляет данные

    :param dict: route_dict
    :param list: booking_token_list
    :return None
    """

    headers = {
        'Content-Type': 'application/json'}

    endpoint = " https://booking-api.skypicker.com/api/v0.1/check_flights?"
    json_response = None
    for items_in_tuple in booking_token_list:
        key, token = items_in_tuple
        query = endpoint + \
            f"v=2&booking_token={token}" + \
            "&bnum=3&pnum=2&affily=picky_{market}&currency=KZT&adults=1&children=0&infants=0"


        json_response = thread_pool(process,query)

        flights_checked = json_response['flights_checked']
        flights_invalid = json_response['flights_invalid']
        price_change = json_response['flights_checked']

        print(f'Flights_checked: [{flights_checked}]\
                Flights_invalid: [{flights_invalid}]\
                Price change: [{price_change}] {key}')

        if flights_invalid:
            print(
                f'[INFO]: Flights_invalid:{key} {flights_invalid}\n[INFO]: START update data ')
            route_dict[key].append({'flights_invalid': flights_invalid})
            create_cache(route_dict)
        elif not flights_checked:
            print('[INFO]: Not all tickets are confirmed\n[INFO]: START update data')
            check_valid_ticket(route_dict, booking_token_list)
        elif price_change:
            print(
                f'[INFO]: Price change:[{price_change}] {key}\n[INFO]: START update data')
            update_data = get_data_from_api()[0]
            create_cache(update_data)
            print(f'[INFO]:Update data finish')


def main():
    '''
    Функция запуска

    :return: None
    '''

    data, booking_token_list = get_data_from_api()
    while True:
        check_valid_ticket(data, booking_token_list)
        time.sleep(1)


main()
