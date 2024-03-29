from concurrent.futures import as_completed
from datetime import datetime
from datetime import timedelta

from requests_futures.sessions import FuturesSession

from data_structures import BlockDay, default_headers
from project_enum import RetrievalType

default_maintain_from = '2021-01-01'
api_url = 'server.bitsplorer.org'
api_port = 443


def date_range_calc(maintain_from):
    from_date = datetime.strptime(maintain_from, '%Y-%m-%d')
    current_date = datetime.utcnow()
    date_delta = current_date - from_date
    date_list = []
    for day in range(date_delta.days):
        working_date = from_date + timedelta(days=day)
        date_list.append(working_date.strftime('%Y-%m-%d'))

    return date_list


if __name__ == '__main__':
    maintain_from = input("Please enter a date to maintain from (iso format): ")
    working_range = date_range_calc(maintain_from)
    futures_list = []

    with FuturesSession() as session:
        for date in working_range:
            url = f'{api_url}:{api_port}/api/blockdays?date={date}'
            print('Requesting data from ', url)
            futures_list.append(session.get(url, headers=default_headers, timeout=30))

    for future in as_completed(futures_list):
        try:
            result = future.result()
            result.raise_for_status()
        except Exception as ex:
            print('Date not available from backend', ex)
        else:
            original_date = result.request.url[-10::]
            print(f'Original Date is {original_date}')
            print(f'Removing {original_date} from working list')
            working_range.remove(original_date)

    print(f'{len(working_range)} entries require retrieval')

    for date in working_range:
        BlockDay(datetime.strptime(date, '%Y-%m-%d')).data_retrieval(RetrievalType.OUTLINE_ONLY)
