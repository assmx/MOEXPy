from datetime import date, datetime, timedelta
import pandas as pd
import MOEXPy


def futoi(symbol, from_date=None, to_date=None, latest=0, data_format='json'):
    """Открытые позиции по фьючерсному контракту

    :param str symbol: Фьючерс. https://www.moex.com/ru/derivatives/open-positions-online.aspx
    :param date|None from_date: Дата, начиная с которой отдаются данные
    :param date|None to_date: Дата, которой будет заканчиваться интервал
    :param int latest: 0 - Получение данных раз в 5 минут, 1 - Получение данных раз в день
    :param str data_format: Формат выдачи данных: json/xml/csv
    """
    params = {'latest': latest}
    if from_date:
        params['from'] = from_date
    if to_date:
        params['till'] = to_date
    return MOEXPy.get_request(f'/analyticalproducts/futoi/securities/{symbol}.{data_format}', params)

    # from_str = f'&from={from_date}' if from_date else ''  # Формат дат начала и окончания
    # to_str = f'&till={to_date}' if to_date else ''   # YYYY-MM-DD
    # url = f'{self.url}/analyticalproducts/futoi/securities/{symbol}.{data_format}?{from_str}{to_str}&latest={latest}'
    # return requests.get(url, headers=self.headers, cookies=self.cookies)


def futoi_to_dataframe(symbol, from_date=None, to_date=None, latest=0):
    """Открытые позиции по фьючерсному контракту в виде pandas DataFrame

    :param str symbol: Фьючерс. https://www.moex.com/ru/derivatives/open-positions-online.aspx
    :param date|None from_date: Дата, начиная с которой отдаются данные
    :param date|None to_date: Дата, которой будет заканчиваться интервал
    :param int latest: 0 - Получение данных раз в 5 минут, 1 - Получение данных раз в день
    """
    t = to_date if to_date else datetime.today().date()  # Дату окончания интервала запроса ставим или указанную, или текущую
    result = pd.DataFrame()
    while not from_date or from_date <= t:  # Пока нужно делать запрос
        json = futoi(symbol, from_date, t, latest).json()  # Полученный ответ переводим в формат JSON, чтобы импортировать в pandas DataFrame
        df = pd.DataFrame.from_dict(data=json['futoi']['data'])
        if df.empty:
            df = pd.DataFrame(columns=json['futoi']['columns'])
        else:
            df.columns = json['futoi']['columns']

        result = pd.concat([result, df]).drop_duplicates(keep='last')  # Объединяем существующие данные с полученными, убираем дубликаты

        if not from_date:
            break
        from_date += timedelta(days=1)

    result['trade_datetime'] = pd.to_datetime(result['tradedate'] + ' ' + result['tradetime'])
    result['system_datetime'] = pd.to_datetime(result['systime'])
    result = result[['sess_id', 'seqnum', 'trade_datetime', 'ticker', 'clgroup', 'pos', 'pos_long', 'pos_short', 'pos_long_num', 'pos_short_num', 'system_datetime']]
    # if from_date:  # Если указали дату начала
    #     from_dt = datetime.combine(from_date, datetime.min.time())  # Дату начала переводим в дату в время 00:00:00
    #     result = result[result['trade_datetime'] >= from_dt]  # Убираем данные до этой даты
    result.sort_values(['trade_datetime', 'clgroup'], inplace=True)  # Сортируем по возрастанию даты и физ/юр
    result.reset_index(drop=True, inplace=True)  # Перестраиваем индекс
    return result
