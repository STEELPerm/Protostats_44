"""
Это временное решение, пока не наладится парсер на C#
Этот проект использует ту же хранимку для получения и синхронизации протоколов, что и парсер на 65-ом сервере
Ну почти. Запрос, с pyodbc нельзя вернуть результаты работы хранимки, поэтому здесь есть модифицированная копия
"""
import pandas as pd
import requests
import pyodbc
import time
import datetime as dt
import traceback
import numpy as np
from bs4 import BeautifulSoup
import urllib3
from multiprocessing.dummy import Pool as ThreadPool
import random
import sys
# sys.path.insert(0, './')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Скрытые модули: lxml, html5lib, webencodings
# pyinstaller работает криво, поэтому
# ЕСЛИ EXE ФАЙЛ НЕ ЗАПУСКАЕТСЯ ПЕРЕБРОСИТЬ В ПАПКУ С ВЫГРУЖЕННЫМ .exe ЭТИ МОДУЛИ ИЗ 64/venv/Lib/site-packages/
try:
    import lxml
    import html5lib
    print('HTML5: ',html5lib.__version__)
    print('LXML: ',lxml.__version__)
except:
    traceback.print_exc()
    time.sleep(15)

def sql_login(sql_file='sql_login.txt'):
    try:
        with open(sql_file) as f:
            t0 = f.read().split('\n')
            login_sql = t0[0]
            password_sql = t0[1]
    except:
        print('Попытка авторизации в базе провалилась.')
        time.sleep(60)
        sys.exit()
    return login_sql, password_sql

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def select_query(query, login, password, DB = 'Cursor', isList = False):
    time.sleep(0.5)  # Спим, чтобы не травмировать базу
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                               'Server=37.203.243.65\CURSORMAIN,49174;'
                               'Database=' + DB + ';'
                               'UID='+login+';'
                               'PWD='+password+';'
                               'Trusted_Connection=no;')
    df = pd.read_sql_query(query, conn)
    conn.close()
    if isList == True:
        list_df = df.values
        flat_list = [item for sublist in list_df for item in sublist]
        return flat_list  # Возвращаем лист
    else:
        return df  # Возвращаем Dataframe pandas

# Привязка ИНН и КПП на импорте, согласно оргу, полученному на сайте
def find_org_in_base(org_string):  # Функция необходимая, но очень медленная
    inn0 = None
    kpp0 = None

    # Чистим string
    org_string = org_string.replace('»', '"').replace('«', '"')
    string_edited = org_string.replace('  ', ' ').replace('- ', '-').replace(' -.', '').replace(' -', '-').replace(' - ', '-')\
        .replace('" ', '"')
    string_edited2 = string_edited.replace('"', '')
    string_edited3 = string_edited.replace('общество с ограниченной ответственностью ', '')

    # Этот метод хуже для базы, но в разы быстрее итерации через список
    org_query2 = "SELECT DISTINCT INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + org_string + "%' or OrgNmS like '%" + org_string + "%'"
    df_org2 = select_query(org_query2, login_sql, pass_sql)

    if df_org2.empty == True:
        org_query2_1 = "SELECT DISTINCT INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited + "%' or OrgNmS like '%" + string_edited + "%'"
        df_org2 = select_query(org_query2_1, login_sql, pass_sql)

    if df_org2.empty == True:  # Ищем по второму едиту
        org_query3 = "SELECT DISTINCT INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited2 + "%' or OrgNmS like '%" + string_edited2 + "%'"
        df_org2 = select_query(org_query3, login_sql, pass_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT DISTINCT INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3 + "%' or OrgNmS like '%" + string_edited3 + "%'"
        df_org2 = select_query(org_query3, login_sql, pass_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT DISTINCT INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited + "%'"
        df_org2 = select_query(org_query4, login_sql, pass_sql)

    if df_org2.empty == False:  # Если нашли - заменяем None
        print(org_string)
        inn0 = df_org2['INN'][0]
        kpp0 = df_org2['KPP'][0]

    return inn0, kpp0

# Находим индекс участника
def find_index(df, list_of_strings, column_int):
    ind_to_return = None
    for ind, row in df.iterrows():
        if any(ext in list_of_strings for ext in str(row[df.columns[column_int]]).lower()):
            ind_to_return = ind
    return ind_to_return

# Анализ таблиц в html и закачка данных в базу
def parse_and_load_data(proxy0, time_to_sleep, url0, headers0, notif_number, login, password):
    time.sleep(random.uniform(0.1,0.5))
    isDouble = False  # два участника, если False - один участник

    z = None  # статус заявки
    main_stat = None  # результат работы парсера
    prot_num = 0  # номер протокола (не учитывается)
    winner = None  # победитель
    sec_winner = None  # второй участник
    price = None  # цена победителя
    sec_price = None  # цена второго участника
    inn = None  # инн победителя
    kpp = None  # кпп победителя
    inn2 = None
    kpp2 = None

    time.sleep(time_to_sleep)
    print('Подключаюсь, используя прокси: ' + str(proxy0))
    r = requests.post(url0, headers=headers0, verify=False, proxies=proxy0,
                      timeout=10)  # Пробуем подключиться
    try:
        df = pd.read_html(r.text, header=0)  # Берем таблицы из html
        if len(df) > 1:  # Если количество таблиц больше одной
            df_win = df[1]  # берем вторую таблицу, поскольку в первой - заказчик

            isNone = False  # Проверяем если nan первая строка
            try:
                np.isnan(df_win[df_win.columns[0]][0])  # У них сейчас в таблицах первая строка - это nan | причина или первая строкаа - сразу поставщик
                isNone = True
            except:
                pass

            if isNone == True:  # если первая строка первой колонны с nan
                # df_win.to_excel('test.xlsx')  # раскоментить для тестовой выгрузки таблицы
                if 'ни одной' in str(df_win[df_win.columns[1]][0]).lower() or 'отклонен' in str(df_win[df_win.columns[1]][0]).lower():  # первая строка второй колонны. Проверяем если ни одной заявки

                    z = str(df_win[df_win.columns[1]][0]).lower()
                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'

                elif 'только одна' in str(
                        df_win[df_win.columns[1]][0]).lower() or 'единствен' in str(df_win[df_win.columns[1]][0]).lower():  # Проверяем если только одна заявка

                    winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')
                    inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower())
                    price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')
                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                    prot_num = 1

                elif 'двух и более' in str(df_win[df_win.columns[1]][0]).lower():

                    winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')
                    inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower())
                    price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')

                    sec_winner = str(df_win[df_win.columns[0]][2]).lower().replace('"', '')
                    inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][2]).lower())
                    sec_price = str(df_win[df_win.columns[2]][2]).replace(' ', '').replace(',', '.')

                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                    isDouble = True

            elif len(df_win) > 1 and isNone == False:  # Если количество строк в таблице больше одной

                ind_win = find_index(df_win, ['побед', 'перв', '1'], 1)
                if ind_win != None:
                    winner = str(df_win[df_win.columns[0]][ind_win]).lower().replace('"', '')
                    inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][ind_win]).lower())
                    price = str(df_win[df_win.columns[2]][ind_win]).replace(' ', '').replace(',', '.')
                else:
                    winner = str(df_win[df_win.columns[0]][0]).lower().replace('"', '')
                    inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][0]).lower())
                    price = str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',', '.')

                ind_sec = find_index(df_win, ['втор', '2'], 1)
                if ind_sec != None:
                    sec_winner = str(df_win[df_win.columns[0]][ind_sec]).lower().replace('"', '')
                    inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][ind_sec]).lower().lower())
                    sec_price = str(df_win[df_win.columns[2]][ind_sec]).replace(' ', '').replace(',', '.')
                else:
                    sec_winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')
                    inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower().lower())
                    sec_price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')

                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                isDouble = True

        elif len(df) == 1:  # Если одна таблица в документе
            df0 = df[0]
            info0 = df0[df0.columns[0]][0]  # берем ячейку, в которой статус
            if 'отсутствуют' in info0.lower():
                z = 'ПОДАЧА ЗАЯВОК'
                main_stat = z
            elif 'отменен' in info0.lower():
                z = 'ЗАКУПКА ОТМЕНЕНА'
                main_stat = z

        # DEBUG. Если нужно - раскомментить
        print(z)
        print(main_stat)
        print(winner)
        print(inn)
        print(price)
        print(sec_winner)
        print(inn2)
        print(sec_price)
    except:
        # traceback.print_exc()  # Раскомментить для вывода ошибок
        print('Нет таблиц в документе')
        # Находим статус заявки по классу, если таблиц нет
        parsed_html = BeautifulSoup(r.text, features="html.parser")
        status = parsed_html.findAll("span", {"class": 'cardMainInfo__state distancedText'})[0].text
        print(status)
        if 'подача' in status.lower():
            z = 'ПОДАЧА ЗАЯВОК'
            main_stat = z
        elif 'отменен' in status.lower():
            z = 'ЗАКУПКА ОТМЕНЕНА'
            main_stat = z
        elif 'комиссии' in status.lower():
            z = 'РАБОТА КОМИССИИ'
            main_stat = z
        else:
            z = 'НЕИЗВЕСТНАЯ ОШИБКА'
            main_stat = z

    # Загрузка на импорт
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=37.203.243.65\CURSORMAIN,49174;'
                          'Database=CursorImport;'
                          'UID='+login+';'
                          'PWD='+password+';'
                          'Trusted_Connection=no;')
    cursor = conn.cursor()
    if isDouble == False:
        cursor.execute(
            "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, winner, second_org, create_dt, why_not, price, main_stat, INN, KPP)"
            "values (?, ?, ?, ?, getdate(), ?, ?, ?, ?, ?) ",
            notif_number, prot_num, winner, sec_winner, z, price, main_stat, inn, kpp
        )
    else:
        cursor.execute(
            "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, winner, create_dt, why_not, price, main_stat, INN, KPP)"
            "values (?, ?, ?, getdate(), ?, ?, ?, ?, ?) ",
            notif_number, 1, winner, z, price, main_stat, inn, kpp
        )
        cursor.execute(
            "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, second_org, create_dt, why_not, price, main_stat, INN, KPP)"
            "values (?, ?, ?, getdate(), ?, ?, ?, ?, ?) ",
            notif_number, 2, sec_winner, z, sec_price, main_stat, inn2, kpp2
        )
    conn.commit()
    conn.close()

# Функция для попыток парсера подключиться к закупкам
def parser_work(chunk):
    good_proxies = []
    bad_proxies = []
    conn_timeout = []
    i0 = 1
    for n in chunk:
        notif = n
        print('Thread № ', i0, '/', 'Номер в списке: ', args.index(n) + 1, ' из ', len(args))  # Тут глобальные переменные. Могут ломаться

        url = "https://zakupki.gov.ru/epz/order/notice/ea44/view/supplier-results.html?regNumber=" + notif
        print(url)
        isnotdone = True
        for i in range(0, 10):
            while isnotdone == True:
                isError = False

                try:

                    headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'Content-type': 'application/json; charset=UTF-8',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                    }
                    try:
                        for p1 in proxs:
                            if p1 not in conn_timeout and p1 not in bad_proxies and isnotdone == True:
                                parse_and_load_data(p1, 0.5, url, headers, notif, login_sql, pass_sql)

                                good_proxies.append(p1)
                                isnotdone = False
                                print('Done')

                        if isnotdone == True:
                            for p1 in conn_timeout:
                                if p1 not in bad_proxies and isnotdone == True:
                                    parse_and_load_data(p1, 1, url, headers, notif, login_sql, pass_sql)

                                    good_proxies.append(p1)
                                    isnotdone = False
                                    print('Done')
                    except requests.exceptions.Timeout as e:
                        print('Connection timeout')
                        isError = True
                        conn_timeout.append(p1)
                        if p1 in good_proxies:
                            good_proxies.remove(p1)
                    except requests.exceptions.ConnectionError as e1:
                        print('Bad proxy')
                        isError = True
                        bad_proxies.append(p1)
                        if p1 in good_proxies:
                            good_proxies.remove(p1)

                except:
                    if isError == True:
                        pass
                    else:
                        # traceback.print_exc()
                        isnotdone = False
        i0 += 1


if __name__ == '__main__':
    startTime = dt.datetime.now() - dt.timedelta(days=3)  # Забираем сегодняшнюю дату из открытой библитеки
    t2 = startTime.strftime('%Y%m%d')  # Форматирование даты №2

    login_sql, pass_sql = sql_login()  # Находим логин и пароль от базы

    proxy_query = "SELECT TOP (3000)[proxy] FROM [CursorImport].[proxy].[Proxys] where dtcreate >= '" + t2 + "'"
    df_proxy = select_query(proxy_query, login_sql, pass_sql)  # Забираем прокси
    proxs = []
    for index, p in df_proxy.iterrows():
        proxy = p['proxy']
        prox = str(proxy).replace(' ', '')
        proxies0 = {'http': 'http://' + prox, 'https': 'http://' + prox, }
        proxs.append(proxies0)
    print('Прокси успешно добыты')

    if len(sys.argv) > 1:  # Достаем номера через консоль (если он был дан)
        args = []
        i = 1
        ln = len(sys.argv)
        while i < ln:
            arguments = sys.argv[i]
            args.append(arguments)
            i += 1
    else:  # Достаем номера ерез хранимку
        print('Добываем номера извещений для обработки')
        work_query = "BEGIN SET NOCOUNT ON; select distinct replace(R.NotifNr, '№ ', '') from [VM1-12\CURSORRPT].[Cursor_rpt_LK].[dbo].ComplexRpt_CACHE R(nolock) left join [VM1-12\CURSORMAIN].[CursorImport].[dbo].import_protocols44 P (nolock) on P.notifnr = replace(R.NotifNr, '№ ', '') where cast(R.TendDt as date) between cast(dateadd(MM, -1, getdate()) as date) and cast(dateadd(dd, -4, getdate()) as date) and R.StatusT_ID = 7 and R.FZ_FK = 44 and P.notifnr is null union select distinct t.NotifNr from [Cursor].dbo.Tender t (nolock) join [Cursor].dbo.Lot l (nolock) on t.Tender_ID = l.Tender_ID left join [CursorImport].dbo.import_protocols44 p (nolock) on p.notifnr = t.NotifNr and p.winner is not null where LotStatus=4 and isnull(l.PriceMax, 0) = 0 and cast(WinnerSetDt as date) < cast(dateadd(dd, -3 , getdate()) as date) and len(t.NotifNr) = 19 and isnull(p.price, 0) = 0 and l.TypeReasonFail_ID is null and (LotStatus = 2 or LotStatus = 7) union select distinct t.NotifNr from [Cursor].dbo.Tender t (nolock) join [Cursor].dbo.Lot l (nolock) on t.Tender_ID = l.Tender_ID join [Cursor].dbo.LotSpec ls (nolock) on ls.Lot_ID = l.Lot_ID left join [CursorImport].dbo.import_protocols44 p (nolock) on p.notifnr = t.NotifNr and p.winner is not null and p.price > 0 where p.notifnr is null and t.TendDt > dateadd(MM, -6 , getdate()) and len(t.NotifNr) = 19 and LotStatus != 4 and l.TypeReasonFail_ID is null and (LotStatus = 2 or LotStatus = 7) union select distinct t.NotifNr from [Cursor].dbo.Tender t(nolock) join [Cursor].dbo.Lot l (nolock) on t.Tender_ID = l.Tender_ID join [Cursor].dbo.LotSpec ls (nolock) on ls.Lot_ID = l.Lot_ID left join [CursorImport].dbo.import_protocols44 p (nolock) on p.notifnr = t.NotifNr and p.winner is not null and p.price > 0 where p.notifnr is not null and t.TendDt > dateadd(MM, -6 , getdate()) and len(t.NotifNr) = 19 and TypeReasonFail_ID is null and l.Winner_ID is null and (LotStatus = 2 or LotStatus = 7) END"
        df_notifs = select_query(work_query, login_sql, pass_sql, 'CursorImport', True)
        args = df_notifs

    print('Всего протоколов на обработку: ', len(args))
    # Делим на чанки и запускаем многопоточность
    num = 4
    chunks = list(chunks(args, num))
    pool = ThreadPool(4)
    pool.map(parser_work, chunks)
    pool.close()


