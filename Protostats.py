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
#from fuzzywuzzy import fuzz
import random
import sys
import re

import Org_creator

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


def isNaN(num):
    return num != num

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


def sql_server(sql_server_file='sql_server.txt'):
    try:
        with open(sql_server_file) as f:
            t0 = f.read().split('\n')
            driver_sql = t0[0]
            server_sql = t0[1]
            NeedProxy = t0[2]
    except:
        print('Попытка авторизации в базе провалилась.')
        time.sleep(60)
        sys.exit()
    return driver_sql, server_sql, NeedProxy

def getSupINN_KPP (url, headers):
    # Сначала получить ссылку из https://zakupki.gov.ru/epz/order/notice/rpec/search-results.html?orderNum=0372100003422000804
    # <a href="/epz/order/notice/rpec/common-info.html?regNumber=03721000034220008040001" target="_blank">Сведения процедуры заключения контракта</a>
    r_link = requests.post(url, headers=headers, verify=False, proxies='', timeout=30)
    parsed_html_link = BeautifulSoup(r_link.text, features="html.parser")
    next_url = parsed_html_link.find("a")

    # Если ссылки нет, значит "Сведения о процедуре заключения контракта отсутствуют"
    if next_url:
        next_url = next_url.get('href')
        url = 'https://zakupki.gov.ru' + next_url
        print(next_url, url)
    else:
        print(url, "Сведения о процедуре заключения контракта отсутствуют")
        return None, None


    html = requests.get(url, headers=headers)

    #STEEL от 24.11.2022 Бывает, что ссылка заканчивается на 0002 вместо 0001
    # if html.status_code != 200:
    #     print(html.status_code, 'getSupINN_KPP RE_GET')
    #     a = url.index(url[len(url) - 1])
    #     url_end = url[a]
    #
    #     if url_end == '1':
    #         url = url.replace(url[a], '2')
    #         print('getSupINN_KPP new url:', url)
    #         html = requests.get(url, headers=headers)
    #         print(html.status_code, 'getSupINN_KPP RE_GET+')
    #########################################################################

    soup = BeautifulSoup(html.text, 'html.parser')
    items = soup.find_all('span')
    index = -1
    inn_out = None
    kpp_out = None

    # STEEL от 19.08.2022 определение через Вид поставщика 1 - Юридическое лицо, 2 - Физическое лицо
    type_sup = 1


    for item in items:
        # Вид: Юридическое лицо
        if 'Полное наименование поставщика' in item:
            index = items.index(item)
            type_sup = 1
            break

        # Вид: Физическое лицо
        if 'Фамилия, имя, отчество' in item:
            index = items.index(item)
            type_sup = 2
            break


    if type_sup == 1:
        items = items[index + 2:]
    elif type_sup == 2:
        # items = items[index + 4:]

        # STEEL от 25.10.2022 Бывает, что у ИП не пишут: Является индивидуальным предпринимателем: Да
        sp = 2
        for item in items:
            if 'Является индивидуальным предпринимателем' in item:
                sp = 4
                break

        items = items[index + sp:]

    index = -1

    for item in items:
        if 'ИНН' in item:
            index_inn = items.index(item) + 1
            inn_out = items[index_inn].get_text(strip=True)

        if 'КПП' in item:
            index_kpp = items.index(item) + 1
            kpp_out = items[index_kpp].get_text(strip=True)


    return inn_out, kpp_out
    #return items[index_inn].get_text(strip=True), items[index_kpp].get_text(strip=True)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def select_query(query, login, password, driver_sql, server_sql, DB = 'Cursor', isList = False):
    time.sleep(0.5)  # Спим, чтобы не травмировать базу
    conn = pyodbc.connect('Driver='+driver_sql+';'
                               'Server='+server_sql+';'
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
    org_string = org_string.replace('»', '"').replace('«', '"').replace("'", "")
    string_edited = org_string.replace('  ', ' ').replace('- ', '-').replace(' -.', '').replace(' -', '-').replace(' - ', '-')\
        .replace('" ', '"').replace('""', '"').replace('“', '"').replace('”', '"')
    string_edited2 = string_edited.replace('"', '')
    string_edited3 = string_edited.replace('общество с ограниченной ответственностью ', '').replace('общество с огранниченной ответственностью ', '')
    string_edited3_1 = string_edited.replace('общество с ограниченной ответственностью', '')
    string_edited4 = string_edited.replace('общество с ограниченной ответственностью ', '').replace('общество с огранниченной ответственностью ', '').replace('"', '')
    string_edited5 = string_edited.replace('открытое акционерное общество ', '').replace('"', '')
    string_edited6 = string_edited.replace('закрытое акционерное общество ', '').replace('"', '')
    string_edited7 = string_edited.replace('казенное ', '').replace('"', '')
    string_edited8 = string_edited.replace('акционерное общество ', '').replace('"', '')
    string_edited9 = string_edited.replace(' ч.', '').replace('ч', '')
    string_edited10 = string_edited.replace('ооо ', '').replace('ООО ', '').replace('оoo', '').replace('ooo', '').replace('ЗАО ', '').replace('зао ', '')
    string_edited11 = string_edited.replace(' ооо', '').replace(' ООО', '')

    #print('str=',string_edited, string_edited10)
    # Этот метод хуже для базы, но в разы быстрее итерации через список
    org_query2 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + org_string + "%' or OrgNmS like '%" + org_string + "%' order by isnull(isZakupki,0) desc"
    df_org2 = select_query(org_query2, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем как есть. Дальше ищем пока не нашли
        org_query2_1 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited + "%' or OrgNmS like '%" + string_edited + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query2_1, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем по второму едиту
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited2 + "%' or OrgNmS like '%" + string_edited2 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3 + "%' or OrgNmS like '%" + string_edited3 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query4, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3.replace(' ', '') + "%' or OrgNmS like '%" + string_edited3 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры и кавычек
        #print('Org:'+string_edited4+'End')
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited4 + "')+'%' or OrgNmS like '%" + string_edited4 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры ОАО
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited5 + "')+'%' or OrgNmS like '%" + string_edited5 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры ЗАО
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited6 + "')+'%' or OrgNmS like '%" + string_edited6 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания "казенное"
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + string_edited7 + "%' or OrgNmS like '%" + string_edited7 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited.replace('ё', 'е') + "%' or OrgNmS like '%" + string_edited.replace('ё', 'е') + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query4, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры АО
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited8 + "')+'%' or OrgNmS like '%" + string_edited8 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited9.replace('ё', 'е') + "%' or OrgNmS like '%" + string_edited9.replace('ё', 'е') + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query4, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited10 + "%' or OrgNmS like '%" + string_edited10 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3_1 + "%' or OrgNmS like '%" + string_edited3_1 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT  INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited11 + "%' or OrgNmS like '%" + string_edited11 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = select_query(org_query3, login_sql, pass_sql, driver_sql, server_sql)

    # Дистанция Левенштейна плохо работает на 180к строках.
    # if df_org2.empty == True:  # Если не нашли - ищем среди всех
    #     print('here')
    #     Found = False
    #     for ind, row in df_orgs_all.iterrows():
    #         orgnm = row['OrgNm']
    #         inn = row['INN']
    #         kpp = row['KPP']
    #
    #         compare1 = fuzz.partial_ratio(orgnm.lower(), org_string.lower())
    #         print(compare1)
    #         if compare1 > 99:
    #             inn0 = inn
    #             kpp0 = kpp
    #             Found = True
    #
    #         if Found == True:
    #             break

    if df_org2.empty == False:  # Если нашли - заменяем None
        print(org_string)
        inn0 = df_org2['INN'][0]
        kpp0 = df_org2['KPP'][0]

    # в противном случаем возвращаем None. Однако, тогда хранимка не сможет привязать победителей.

    return inn0, kpp0

# Находим индекс участника
def find_index(df, list_of_strings, column_int):
    ind_to_return = None
    for ind, row in df.iterrows():
        if any(x in list_of_strings for x in str(row[df.columns[column_int]]).lower()):
            ind_to_return = ind
    return ind_to_return

# Анализ таблиц в html и закачка данных в базу
def parse_and_load_data(proxy0, time_to_sleep, url0, headers0, notif_number, login, password, driver_sql, server_sql, NeedProxy):
    time.sleep(random.uniform(0.8,1.5))
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
    ident = 0

    time.sleep(time_to_sleep)

    if NeedProxy == 'True':
        print('Подключаюсь, используя прокси: ' + str(proxy0))
    #else:
        #print('Подключаюсь без прокси')


    #print(proxy0)proxy0
    try:
        r = requests.post(url0, headers=headers0, verify=False, proxies=proxy0,
                      timeout=30)  # Пробуем подключиться
    except:
        print('Ошибка')
    try:

        #Если есть на странице "Основание признания торгов несостоявшимися", сразу ставим "По окончании срока подачи заявок не подано ни одной заявки"
        # Не надо такое:  Победитель есть, поскольку единственный участник соответствует требованиям

        # parsed_html = BeautifulSoup(r.text, features="html.parser")
        # html_title = parsed_html.findAll("span", {"class": 'section__title'})
        #
        # i_title = 0
        # while i_title < len(html_title):
        #     if html_title[i_title].text == 'Основание признания торгов несостоявшимися':
        #         z = 'По окончании срока подачи заявок не подано ни одной заявки'
        #         main_stat = z
        #         break
        #     i_title += 1
        #
        # if z == None:

        #STEEL от 10.11.2022 добавил для разделителя . (decimal=',', thousands='.'), чтобы корректно загружал 0.1364, 0.11 и т.п. если на сайте указано 0,1364, 0,11
        df = pd.read_html(r.text, decimal=',', thousands='.', header=0)  # Берем таблицы из html

        if len(df) > 1:  # Если количество таблиц больше одной
            df_win = df[1]  # берем вторую таблицу, поскольку в первой - заказчик

            #print(df_win)
            # print(df_win[df_win.columns[1]][0])
            # print(df_win[df_win.columns[0]][0])
            # print(np.isnan(df_win[df_win.columns[0]][0]))
            # print(len(df_win))
            # print(find_index(df_win, ['побед', 'перв', '1', 'Побед'], 1))
            # print('END')
            # print(find_index(df_win, ['втор', '2'], 1))
            #
            # print(str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',','.'))
            # print (str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',','.'))
            #
            #
            #sys.exit()

            isNone = False  # Проверяем если nan первая строка


            try:
                if np.isnan(df_win[df_win.columns[0]][0]) == True:  # У них сейчас в таблицах первая строка - это nan | причина или первая строкаа - сразу поставщик
                    isNone = True
            except:
                pass

            #p_first = None
            #col = 0
            #try:
            #    p_first = df_win[df_win.columns[0]][0]
            #except:
            #    pass

            #if p_first is None:
            #    isNone = True
            #else:
            #    col = col + 1




            # В какой колонке находится победитель
            k = 0

            while k < 4:
                if 'УЧАСТНИК(И), С КОТОРЫМИ ПЛАНИРУЕТСЯ ЗАКЛЮЧИТЬ КОНТРАКТ' in df_win.columns[k].upper() or 'НАИМЕНОВАНИЕ УЧАСТНИКА' in df_win.columns[k].upper():
                    break
                if 'ИДЕНТИФИКАЦИОННЫЕ НОМЕРА УЧАСТНИКОВ, С КОТОРЫМИ ПЛАНИРУЕТСЯ ЗАКЛЮЧИТЬ КОНТРАКТ' in df_win.columns[k].upper():
                    ident = 1
                    break
                else:
                    k = k + 1

            #print(df_win[df_win.columns[2]][0],k, df_win[df_win.columns[0]][0], 'TEST',find_index(df_win, ['побед', 'перв', '1', 'Побед'], 1),'Len:', len(df_win), isNone, df_win[df_win.columns[k]][0],df_win)
            #sys.exit()

            print('ident ', ident)
            winner = None
            if isNone == True:  # если первая строка первой колонны с nan
                #df_win.to_excel('test.xlsx')  # раскомментить для тестовой выгрузки таблицы

                if 'ни одной' in str(df_win[df_win.columns[1]][0]).lower() or 'отклонен' in str(df_win[df_win.columns[1]][0]).lower():  # первая строка второй колонны. Проверяем если ни одной заявки

                    z = str(df_win[df_win.columns[1]][0]).lower()
                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'

                elif 'только одна' in str(
                        df_win[df_win.columns[1]][0]).lower() or 'единствен' in str(
                    df_win[df_win.columns[1]][0]).lower() or 'только одной второй части заявки на участие в нем' in str(
                    df_win[df_win.columns[1]][0]).lower():  # Проверяем если только одна заявка

                    if df_win[df_win.columns[k]][0] == 'NaN' or df_win[df_win.columns[k]][0] == 'nan' or np.isnan(df_win[df_win.columns[k]][0]) == True:
                        winner = '1'
                        ident = 1
                    else:
                        winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')  # берем победителя из первой колонны,второй строки
                    if ident == 0:
                        inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower())  # находим орг в базе
                    try:
                        price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')  # берем цену победителя из 3 колонны, второй строки
                    except:
                        price = str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',','.')  # берем цену победителя из 3 колонны, второй строки
                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'  # проставляем,что нашли победителя
                    prot_num = 1  # проставляем номер

                elif 'двух и более' in str(df_win[df_win.columns[1]][0]).lower(): #or len(df_win) > 1:
                    ind_win = find_index(df_win, ['побед', 'перв', '1'], 1)  # находим строку, в которой находится победитель
                    if ind_win != None:  # если нашли, то берем из этой строки
                        winner = str(df_win[df_win.columns[0]][ind_win]).lower().replace('"', '')
                        if ident == 0:
                            inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][ind_win]).lower())
                        price = str(df_win[df_win.columns[2]][ind_win]).replace(' ', '').replace(',', '.')
                    else:  # если не нашли, то берем первую строку
                        winner = str(df_win[df_win.columns[0]][0]).lower().replace('"', '')
                        if ident == 0:
                            inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][0]).lower())
                        price = str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',', '.')

                    ind_sec = find_index(df_win, ['втор', '2'], 1)  # находим строку,в которой находится второй участник
                    if ind_sec != None:
                        sec_winner = str(df_win[df_win.columns[0]][ind_sec]).lower().replace('"', '')
                        if ident == 0:
                            inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][ind_sec]).lower().lower())
                        sec_price = str(df_win[df_win.columns[2]][ind_sec]).replace(' ', '').replace(',', '.')
                    else:
                        sec_winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')
                        if ident == 0:
                            inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower().lower())
                        sec_price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')

                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                    isDouble = True

                # Если не указано наименование победителя и нет идент. номера в первой колонке (Nan)
                if 'обедитель' in str(df_win[df_win.columns[1]][0]).lower():  # Проверяем есть ли победитель
                    winner = '1'
                    ident = 1

                    price = str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',','.')  # берем цену победителя из 3 колонны, второй строки

                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'  # проставляем,что нашли победителя
                    prot_num = 1  # проставляем номер

                try:
                    if 'торой номер' in str(df_win[df_win.columns[1]][1]).lower():  # Проверяем есть ли 2й участник
                        sec_winner = '2'
                        ident = 1

                        sec_price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',','.')  # берем цену победителя из 3 колонны, второй строки

                        main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'  # проставляем,что нашли победителя
                        isDouble = True
                except:
                    pass

            elif len(df_win) > 1 and isNone == False:  # Если количество строк в таблице больше одной
                #ind_win = find_index(df_win, ['побед', 'перв', '1'], 1)
                ind_win = find_index(df_win, ['побед', 'перв', '1', 'Побед'], 1)

                # print (k,'TYT', df_win, ind_win, df_win.columns[k])
                # print(df_win.columns[1])
                # sys.exit()

                if ind_win != None:
                    #winner = str(df_win[df_win.columns[0]][ind_win]).lower().replace('"', '')
                    #inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][ind_win]).lower())
                    #price = str(df_win[df_win.columns[2]][ind_win]).replace(' ', '').replace(',', '.')

                    winner = str(df_win[df_win.columns[k]][ind_win]).lower().replace('"', '')
                    if ident == 0:
                        inn, kpp = find_org_in_base(str(df_win[df_win.columns[k]][ind_win]).lower())
                    price = str(df_win[df_win.columns[2+k]][ind_win]).replace(' ', '').replace(',', '.')
                else:
                    #winner = str(df_win[df_win.columns[0]][0]).lower().replace('"', '')
                    #inn, kpp = find_org_in_base(str(df_win[df_win.columns[0]][0]).lower())
                    #price = str(df_win[df_win.columns[2]][0]).replace(' ', '').replace(',', '.')

                    winner = str(df_win[df_win.columns[k]][0]).lower().replace('"', '')
                    if ident == 0:
                        inn, kpp = find_org_in_base(str(df_win[df_win.columns[k]][0]).lower())
                    price = str(df_win[df_win.columns[2+k]][0]).replace(' ', '').replace(',', '.')

                #ind_sec = find_index(df_win, ['втор', '2'], 1)

                #ind_sec = find_index(df_win, ['втор', '2'], 1+col)
                ind_sec = find_index(df_win, ['втор', '2'], 1+k)
                if ind_sec != None:
                    #sec_winner = str(df_win[df_win.columns[0]][ind_sec]).lower().replace('"', '')
                    #inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][ind_sec]).lower().lower())
                    #sec_price = str(df_win[df_win.columns[2]][ind_sec]).replace(' ', '').replace(',', '.')

                    sec_winner = str(df_win[df_win.columns[k]][ind_sec]).lower().replace('"', '')
                    if ident == 0:
                        inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[k]][ind_sec]).lower().lower())
                    sec_price = str(df_win[df_win.columns[2+k]][ind_sec]).replace(' ', '').replace(',', '.')
                else:
                    #sec_winner = str(df_win[df_win.columns[0]][1]).lower().replace('"', '')
                    #inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[0]][1]).lower().lower())
                    #sec_price = str(df_win[df_win.columns[2]][1]).replace(' ', '').replace(',', '.')

                    try:
                        sec_winner = str(df_win[df_win.columns[k]][1]).lower().replace('"', '')
                        if ident == 0:
                            inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[k]][1]).lower().lower())
                        sec_price = str(df_win[df_win.columns[2+k]][1]).replace(' ', '').replace(',', '.')
                    except:
                        pass

                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                isDouble = True

            # STEEL от 31.05.2021 добавил блок, если len(df_win) == 1
            elif len(df_win) == 1 and isNone == False:  # Если количество строк в таблице больше одной
                #ind_win = find_index(df_win, ['побед', 'перв', '1'], 1)
                ind_win = find_index(df_win, ['побед', 'перв', '1', 'Побед'], 1)

                # print (k,'TYT1', df_win, ind_win, df_win.columns[k])
                # print(df_win.columns[1])
                # sys.exit()

                if ind_win != None:
                    winner = str(df_win[df_win.columns[k]][ind_win]).lower().replace('"', '')
                    if ident == 0:
                        inn, kpp = find_org_in_base(str(df_win[df_win.columns[k]][ind_win]).lower())
                    price = str(df_win[df_win.columns[2+k]][ind_win]).replace(' ', '').replace(',', '.')
                else:
                    winner = str(df_win[df_win.columns[k]][0]).lower().replace('"', '')
                    if ident == 0:
                        inn, kpp = find_org_in_base(str(df_win[df_win.columns[k]][0]).lower())
                    price = str(df_win[df_win.columns[2+k]][0]).replace(' ', '').replace(',', '.')

                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                prot_num = 1  # проставляем номер

            if winner == None:
                m = 0
                p_number = 0
                win_id = 0
                while m < len(df_win):
                    if p_number == 0 and isNaN(df_win[df_win.columns[k]][m]) == False:
                        winner = df_win[df_win.columns[k]][m]
                        if ident == 0:
                            inn, kpp = find_org_in_base(str(df_win[df_win.columns[k]][m]).lower())
                        price = str(df_win[df_win.columns[2 + k]][m]).replace(' ', '').replace(',', '.')
                        p_number = 1
                        win_id = m
                    if p_number == 1 and win_id < m and isNaN(df_win[df_win.columns[k]][m]) == False:
                        sec_winner = df_win[df_win.columns[k]][m]
                        if ident == 0:
                            inn2, kpp2 = find_org_in_base(str(df_win[df_win.columns[k]][m]).lower())
                        sec_price = str(df_win[df_win.columns[2 + k]][m]).replace(' ', '').replace(',', '.')
                        p_number = 2
                    m = m + 1

                if p_number == 2:
                    isDouble = True

                if p_number == 1:
                    main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                    prot_num = 1  # проставляем номер

            #print('pobed',winner)
            #print(inn, kpp)
            #print('Price: ',price)
            #print(len(df_win))


        elif len(df) == 1 and 'ЗАКАЗЧИК(И), С КОТОРЫМИ ПЛАНИРУЕТСЯ ЗАКЛЮЧИТЬ КОНТРАКТ' not in df[0].columns[0].upper() :  # Если одна таблица в документе
            # print(df[0][df[0].columns[0]][0] )
            # print(df[0].columns[0])
            # print(df[0])
            # sys.exit()
            df0 = df[0]
            info0 = df0[df0.columns[0]][0]  # берем ячейку, в которой статус
            print(info0.lower())
            if 'отсутствуют' in info0.lower():
                z = 'ПОДАЧА ЗАЯВОК'
                main_stat = z
            elif 'отменен' in info0.lower():
                z = 'ЗАКУПКА ОТМЕНЕНА'
                main_stat = z
        # DEBUG. Если нужно - раскомментить

        #STEEL от 12.01.2021 Исправил ошибку Error converting data type nvarchar to numeric
        if price != None:
            price = price.replace(' ', '')
            #STEEL от 11.06.2021 добавил условия: 2 фразы и пусто в цене, тогда цена победителя равна НМЦ лота.
            #print (k,price)
            #sys.exit()
            if price == 'nan':
                if '1 - Победитель' in df_win[df_win.columns[k+1]][0]\
                    or (('олько одна заявка' in df_win[df_win.columns[k+1]][0] or 'олько одного участника' in df_win[df_win.columns[k+1]][0])\
                    and ('несостоявш' in df_win[df_win.columns[k+1]][0] or 'признана соответствующей' in df_win[df_win.columns[k+1]][0])):
                    query_price = "select top 1 isnull(TenderPrice,0) from Tender (nolock) where notifnr='"+notif_number+"'"
                    price_rez = select_query(query_price, login_sql, pass_sql, driver_sql, server_sql, 'Cursor', True)
                    if price_rez != None:
                       price = price_rez[0]
                    else:
                        price = 0

        if sec_price == 'nan':
            sec_price = None

        if sec_price != None:
            sec_price = sec_price.replace(' ', '')

        #STEEL от 26.02.2021 Если нет победителя и статус "определение поставщика завершено", то значит: статус "Не сост", а причина "Не подано ни одной заявки".
        #if len(df_win) == 0 and winner == None:
        # print(len(df) , df[0].columns[0].upper() , winner, df)
        # sys.exit()

        if len(df) >= 1 and 'ЗАКАЗЧИК(И), С КОТОРЫМИ ПЛАНИРУЕТСЯ ЗАКЛЮЧИТЬ КОНТРАКТ' in df[0].columns[0].upper() and winner == None:
            parsed_html = BeautifulSoup(r.text, features="html.parser")
            status = parsed_html.findAll("span", {"class": 'cardMainInfo__state distancedText'})[0].text
            if 'определение поставщика завершено' in status.lower():
                z = 'По окончании срока подачи заявок не подано ни одной заявки'
                main_stat = z

        # Получение ИНН КПП из блока  "Информация о процедуре заключения контракта" - "Информация о поставщике"
        #url_sup = 'https://zakupki.gov.ru/epz/order/notice/rpec/common-info.html?regNumber=' + str(notif_number) + '0001'
        # STEEL от 28.11.2022 переделал на получение ссылки
        url_sup = 'https://zakupki.gov.ru/epz/order/notice/rpec/search-results.html?orderNum=' + str(notif_number)

        inn_sup, kpp_sup = getSupINN_KPP(url_sup, headers0)

        # try:
        #     inn_sup, kpp_sup = getSupINN_KPP(url_sup, headers0)
        # except:
        #     inn_sup, kpp_sup = None, None

        if inn_sup != None:
            inn = inn_sup
            kpp = kpp_sup

        if inn != None:
            # Если нет орагнизации, то пытаемся загрузить новый орг в базу через Dadata
            org_cust_query = "SELECT Org_ID from [Cursor].[dbo].[Org] (nolock) where INN = '" + str(inn) + "' and KPP = '" + str(kpp) + "'"
            org_cust = select_query(org_cust_query, login_sql, pass_sql, driver_sql, server_sql)

            if org_cust.empty == True:
                org_cust_query = "SELECT Org_ID from [Cursor].[dbo].[Org] (nolock) where INN = '" + str(inn) + "'"
                org_cust = select_query(org_cust_query, login_sql, pass_sql, driver_sql, server_sql)
                if org_cust.empty == True:
                    print('В базе нет организации с ИНН: ' + str(inn) + ', КПП: ' + str(kpp))
                    resp1 = Org_creator.create(inn, login_sql, pass_sql)  # добавляем в базу с помощью функции
                    print(resp1)

        # STEEL от 17.11.2022 Цена победителя по новому алгоритму от Екатерины
        """
        Вычисляем цену победителя по новому алгоритму, если сработает условие по Цене: 
        разделить полученную цену победителя на НМЦ, и полученный результат не входит в рамки от 0,1 до 1
        
        Новый алгоритм:
        1	Поиск по номеру изв	"Если на странице РЕЗУЛЬТАТЫ ОПРЕДЕЛЕНИЯ ПОСТАВЩИКА (ПОДРЯДЧИКА, ИСПОЛНИТЕЛЯ) в разделе 
        Участники, с которыми планируется заключить  контракт стоит только одно значение и оно равно значению 
        на странице ОБЩАЯ ИНФОРМАЦИЯ в разделе Начальная сумма цен единиц товара, работы, услуги, 
        то в Цене побед ставим НМЦ торгов"        
        от 25.11.2022:
        Если значение одно и оно не равно значению на странице ОБЩАЯ ИНФОРМАЦИЯ в разделе Начальная сумма цен единиц товара, работы, услуги, 
        то включаем алгоритм: НМЦ/значению на странице ОБЩАЯ ИНФОРМАЦИЯ в разделе Начальная сумма цен единиц товара, работы, услуги*значение на стр 
        РЕЗУЛЬТАТЫ ОПРЕДЕЛЕНИЯ ПОСТАВЩИКА (ПОДРЯДЧИКА, ИСПОЛНИТЕЛЯ) в разделе Участники, с которыми планируется заключить контракт


        2	Поиск по номеру изв	Если на странице РЕЗУЛЬТАТЫ ОПРЕДЕЛЕНИЯ ПОСТАВЩИКА (ПОДРЯДЧИКА, ИСПОЛНИТЕЛЯ) в разделе 
        Участники, с которыми планируется заключить контракт стоит несколько значений - идем  на страницу 
        ОБЩАЯ ИНФОРМАЦИЯ в раздел Начальная сумма цен единиц товара, работы, услуги, 
        берем значение Начальная сумма цен единиц товара, работы, услуги. 
        Далее, НМЦ тогов делим на значение Начальная сумма цен единиц товара, работы, услуги. Получаем значение Х. 
        Далее возвращаемся на стр РЕЗУЛЬТАТЫ ОПРЕДЕЛЕНИЯ ПОСТАВЩИКА (ПОДРЯДЧИКА, ИСПОЛНИТЕЛЯ) в раздел 
        Участники, с которыми планируется заключить контракт - умножаем полученное значение Х на цену каждого участника
        и получаем суммы Победителя и суммы каждого участника ( в нашей БД их 4, включая Победителя)           
        """

        html_price = BeautifulSoup(r.text, features="html.parser")

        # НМЦ со страницы
        tenderprice = html_price.find_all("span", {"class": 'cardMainInfo__content cost'})[0].text.replace(
            ' ', '').replace(' ', '').replace('₽', '').replace(',', '.').lstrip()


        delta = 0.1

        # Разделить полученную цену победителя на НМЦ
        if price is not None:
            delta = float(price) / float(tenderprice)

        print(notif_number, 'tenderprice=', float(tenderprice), 'delta=', delta)

        # Если полученный результат не входит в рамки от 0,1 до 1 - определение цены по новому алгоритму
        if not (0.1 <= delta <= 1):
            print(notif_number, 'Определение цены по новому алгоритму, delta=' + str(delta) + '. Цена поб. до расчёта=' + str(price))

            # ссылка на общие данные, откуда надо взять "Начальная сумма цен единиц товара, работы, услуги"
            # url_resultsum = 'https://zakupki.gov.ru/epz/order/notice/ea20/view/common-info.html?regNumber=' + str(notif_number)

            # ссылка на печатную форму, откуда надо взять "Начальная сумма цен единиц товара, работы, услуги"
            url_resultsum = 'https://zakupki.gov.ru/epz/order/notice/printForm/view.html?regNumber=' + str(notif_number)

            # Берём значение поля "Начальная сумма цен единиц товара, работы, услуги"
            #resultsum = getresultsum(url_resultsum, headers0, notif_number)

            # Берём значение поля "Начальная сумма цен единиц товара, работы, услуги" из печатной формы
            resultsum = getresultsum2(url_resultsum, headers0)

            print(notif_number, 'resultsum=', resultsum)

            # 2) - Если несколько победителей
            if isDouble:
                # Находим x
                x = float(tenderprice) / resultsum

                print(notif_number, 'x=', x, 'old_price=', price, 'old_sec_price=', sec_price)

                price = float(price) * x
                sec_price = float(sec_price) * x

                print(notif_number, 'price=', price, 'sec_price=', sec_price)

            # 1) - Если только 1 победитель
            else:
                # Если стоит только одно значение и оно равно значению на странице ОБЩАЯ ИНФОРМАЦИЯ в разделе
                # Начальная сумма цен единиц товара, работы, услуги, то в Цене побед ставим НМЦ торгов"
                if int(float(price)) == int(resultsum):
                    price = float(tenderprice)
                    print(notif_number, 'price1=', price)
                else:
                    # от 25.11.2022: Если не равны
                    price = (float(tenderprice) / float(resultsum)) * float(price)
                    print(notif_number, 'price2=', price)

        print(z)
        print(main_stat, notif_number)
        print(winner)
        print(inn)
        print('inn,kpp sup: ', inn_sup, kpp_sup)
        print(price)
        print(sec_winner)
        print(inn2)
        print(sec_price)

        #sys.exit()

    except:
        traceback.print_exc()  # Раскомментить для вывода ошибок
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

        elif 'определение поставщика завершено' in status.lower():
            z = 'По окончании срока подачи заявок не подано ни одной заявки'
            main_stat = z
        else:
            z = 'НЕИЗВЕСТНАЯ ОШИБКА'
            main_stat = z

    #print(notif_number, prot_num, winner, sec_winner, z, price, main_stat, inn, kpp)
    #sys.exit()

    #Загрузка на импорт
    try:
        # Org_ID=748578, OrgNm='В обработке', если нет ИНН. Когда появится контракт - победитель подставится из него
        if ident == 1 and inn_sup == None:
            Winner_Org_ID = 748578
        else:
            Winner_Org_ID = None

        conn = pyodbc.connect('Driver='+driver_sql+';'
                              'Server='+server_sql+';'
                              'Database=CursorImport;'
                              'UID='+login+';'
                              'PWD='+password+';'
                              'Trusted_Connection=no;')
        cursor = conn.cursor()
        if isDouble == False:
            cursor.execute(
                "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, winner, second_org, create_dt, why_not, price, main_stat, INN, KPP, Winner_Org_ID)"
                "values (?, ?, ?, ?, getdate(), ?, ?, ?, ?, ?, ?) ",
                notif_number, prot_num, winner, sec_winner, z, price, main_stat, inn, kpp, Winner_Org_ID
            )
        else:
            cursor.execute(
                "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, winner, create_dt, why_not, price, main_stat, INN, KPP, Winner_Org_ID)"
                "values (?, ?, ?, getdate(), ?, ?, ?, ?, ?, ?) ",
                notif_number, 1, winner, z, price, main_stat, inn, kpp, Winner_Org_ID
            )
            cursor.execute(
                "insert into [CursorImport].[dbo].[import_protocols44](notifnr, prot_num, second_org, create_dt, why_not, price, main_stat, INN, KPP, Winner_Org_ID)"
                "values (?, ?, ?, getdate(), ?, ?, ?, ?, ?, ?) ",
                notif_number, 2, sec_winner, z, sec_price, main_stat, inn2, kpp2, Winner_Org_ID
            )
        conn.commit()
        conn.close()
    except:
        traceback.print_exc()

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
                    UserAgent = GetRandomUserAgent()

                    headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'Content-type': 'application/json; charset=UTF-8',
                        'User-Agent': UserAgent
                        #'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                    }

                    #print(notif, UserAgent)

                    try:
                        if NeedProxy == 'True':
                            for p1 in proxs:
                                if p1 not in conn_timeout and p1 not in bad_proxies and isnotdone == True:
                                    print(p1)
                                    parse_and_load_data(p1, 0.5, url, headers, notif, login_sql, pass_sql, driver_sql, server_sql, NeedProxy)

                                    good_proxies.append(p1)
                                    isnotdone = False
                                    print('Done')
                        else:
                            parse_and_load_data('', 1, url, headers, notif, login_sql, pass_sql, driver_sql, server_sql, NeedProxy)
                            isnotdone = False
                            print('Done')

                        if isnotdone == True and NeedProxy == 'True':
                            for p1 in conn_timeout:
                                if p1 not in bad_proxies and isnotdone == True:
                                    parse_and_load_data(p1, 1, url, headers, notif, login_sql, pass_sql, driver_sql, server_sql, NeedProxy)

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

# STEEL от 23.12.2020 добавляем UserAgent для смены
def GetRandomUserAgent ():
    with open("UserAgent.txt") as file:
        UserAgentAr = [row.strip() for row in file]

    Rand = random.randint(1, len(UserAgentAr)-1)
    return UserAgentAr[Rand]

# STEEL от 17.11.2022 для нахождения цены по новому алгоритму.
# Забирает поле "Начальная сумма цен единиц товара, работы, услуги" из вкладки "Общая информация"
def getresultsum (url, headers, notif_number):

    htmlsum = requests.get(url, headers=headers)
    soup = BeautifulSoup(htmlsum.text, 'html.parser')

    resultsum = soup.find_all("span", {"class": 'cost'})[1].text.replace(' ', '').replace(' ', '').replace('₽', '').replace(',', '.').lstrip()
    otherpage = soup.find_all("div", {"class": 'd-flex justify-content-between align-items-baseline'})

    if otherpage != []:
        print(notif_number, 'Несколько страниц на вкладке с Общими данными')

        # <div id="medTable31293846">
        medTableALL = str(soup.findAll('div', attrs={'id': re.compile("medTable")})[0]).split('<div id="')[1]

        LotId = medTableALL[medTableALL.find('ble') + 3:medTableALL.find('">')]
        url2 = 'https://zakupki.gov.ru/epz/order/notice/ea20/view/medicinesPage.html?pageNumber=1&recordsPerPage=50&lotId=' + str(LotId) + '&regNumber=' + str(notif_number)

        html2 = requests.get(url2, headers=headers)
        soup2 = BeautifulSoup(html2.text, 'html.parser')

        resultsum2 = soup2.find_all("span", {"class": 'cost'})[0].text.replace(' ', '').replace(' ', '').replace('₽',
                                                                             '').replace(',', '.').lstrip()

        if resultsum != resultsum2:
            resultsum = resultsum2


    return float(resultsum)

# STEEL от 22.11.2022 для нахождения цены по новому алгоритму.
# Забирает поле "Начальная сумма цен единиц товара, работы, услуги" из печатной формы
def getresultsum2(url, headers):

    # Просмотр через печатную форму
    # GET https://zakupki.gov.ru/epz/order/notice/printForm/view.html?regNumber=0358300335522000218

    html = requests.get(url, headers=headers)
    soup = BeautifulSoup(html.text, 'html.parser')

    # <td id="invis" colspan="9" align="right">Начальная сумма цен единиц товара: 443.64 Российский рубль</td>
    try:
        resultsum = soup.find_all("td", {"id": 'invis'})[1].text.replace(
                'Начальная сумма цен единиц товара: ', '').replace(
                'Итого по лекарственным препаратам: ', '').replace(
                'Итого:', '').replace(
                ' ', '').replace(' Российский рубль', '').replace(',', '.').replace(' ', '').lstrip()
    except:

        # <p align="right">Начальная сумма цен товара, работы, услуги: 90.00 Российский рубль</p>
        try:
            resultsum = soup.find_all("p", {"align": 'right'})[1].text.replace(
                'Начальная сумма цен товара, работы, услуги: ', '').replace(
                'Итого:', '').replace(
                ' ', '').replace(' Российский рубль', '').replace(',', '.').replace(' ', '').lstrip()
        except:
            resultsum = None


    return float(resultsum)



if __name__ == '__main__':
    startTime = dt.datetime.now() - dt.timedelta(days=3)  # Забираем сегодняшнюю дату из открытой библитеки
    t2 = startTime.strftime('%Y%m%d')  # Форматирование даты №2

    login_sql, pass_sql = sql_login()  # Находим логин и пароль от базы

    driver_sql, server_sql, NeedProxy = sql_server()  # Находим настройки сервера


    proxs = []

    if NeedProxy == 'True':  #STEEL от 23.12.2020 добавил параметр NeedProxy. В файле sql_server.txt

        # proxy_query = "SELECT TOP (3000)[proxy] FROM [CursorImport].[proxy].[Proxys] where dtcreate >= '" + t2 + "'"
        proxy_query = "exec [CursorImport].[dbo].[spProtocols44_GetProxy]"
        df_proxy = select_query(proxy_query, login_sql, pass_sql, driver_sql, server_sql)  # Забираем прокси

        for index, p in df_proxy.iterrows():
            proxy = p['proxy']
            prox = str(proxy).replace(' ', '')
            proxies0 = {'http': 'http://' + prox, 'https': 'http://' + prox, }
            proxs.append(proxies0)
        print('Прокси успешно добыты')
    else:
        print('Запуск без прокси')

    # Добываем все орги из справочника
    org_query_all = "SELECT DISTINCT OrgNm, INN, KPP from [Cursor].[dbo].Org where Cntr_ID = 643 and OrgNm is not null and INN is not null"
    #df_orgs_all = select_query(org_query_all, login_sql, pass_sql, driver_sql, server_sql)

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
        work_query = "exec spProtocols44_GetNotifnrForLoad"
        df_notifs = select_query(work_query, login_sql, pass_sql, driver_sql, server_sql, 'CursorImport', True)
        args = df_notifs

    print('Всего протоколов на обработку: ', len(args))
    # Делим на чанки и запускаем многопоточность
    num = 4
    chunks = list(chunks(args, num))
    pool = ThreadPool(4)
    pool.map(parser_work, chunks)
    pool.close()

