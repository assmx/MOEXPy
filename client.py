import requests
import MOEXPy


def connect(login, password):
    """Подключение к информационно-статистическому серверу Московской Биржи (ИСС/ISS)

    :param str login: Имя пользователя
    :param str password: Пароль
    """
    session = requests.Session()  # Сессия запроса
    session.get('https://passport.moex.com/authenticate', auth=(login, password))  # Basic-аутентификация на сайте moex.com
    MOEXPy.headers = {'User-Agent': session.headers['User-Agent']}  # python-requests/x.xx.x
    MOEXPy.cookies = {'MicexPassportCert': session.cookies['MicexPassportCert']}  # Сертификат аутентификации. Будет отправляться при последующих запросах


def get_request(request_url, params):

    req = requests.models.PreparedRequest()
    req.prepare_url(f'{MOEXPy.url}{request_url}', params)
    return requests.get(req.url, headers=MOEXPy.headers, cookies=MOEXPy.cookies)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    from Config import Config  # Файл конфигурации

    MOEXPy.connect(Config.Login, Config.Password)  # Авторизуемся на Московской Бирже
    security = 'Si'  # Si курс доллар США-российский рубль
    latest = 0  # Получение данных раз в 5 минут
    df = MOEXPy.futoi_to_dataframe(security, latest=latest)  # Последние 1000 значений (по 500 для физических и юридических лиц)
    print(df)
