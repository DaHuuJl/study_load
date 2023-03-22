# -*- coding:utf-8 -*-
# from pandarallel import pandarallel
# from tqdm import tqdm

import multiprocessing
import pandas as pd
import numpy as np
import time
import math

COUNT = 500


def load_data_from_file(file, header=None, names=None):
    df = pd.read_excel(io=file, index_col=None, header=header, names=names)
    # df.info()
    print('Файл - ОК')
    return df


def split_thread(df, prc_i, return_dict):
    print('Разбиение потоков - START')
    # # print(df.to_string())
    # # print(df['Группа'])
    df.insert(5, "Группа ДО", None)
    # Новый датафрейм для записи разбитых потоков
    new_df = df.iloc[0:0]
    # Перебираем таблицу построчно
    for i, row in df.iterrows():
        # print(row['Группа'])
        # Разбиваем столбец ГРУППА по разделителю ','
        group = row[4]
        try:
            arr = row[4].split(', ')
            # Перебираем полученный массив
            for j in range(len(arr)):
                row[5] = group
                # Удаляем последнюю запятую в строке
                new_data = arr[j].replace(',', '')
                # Заменяем поток на строку
                row[4] = new_data
                # Записываем данные в новый датафрейм
                new_df = pd.concat([pd.DataFrame([row.values], columns=df.columns), new_df], ignore_index=True)
        except AttributeError:
            row[4] = "$$$$$$$$$$"
            new_df = pd.concat([pd.DataFrame([row.values], columns=df.columns), new_df], ignore_index=True)
    # print(new_df.tail(20).to_string())
    print('Разбиение потоков - ОК')
    return_dict[prc_i] = new_df
    return new_df


def calculate_time(df):
    # Новый столбец для записи времени потраченного 1 группу из общего количества часов
    df.insert(12, "Часы на группу", None)
    # # print(df.to_string())
    number_str_count = {}
    # Производим подсчёт количества строк для полей с одинаковыми значениями столбца "НомерСтроки" (чтобы узнать на сколько делить время
    print('Подсчёт количества - START')
    for i, row in df.iterrows():
        actual = row['НомерСтроки']
        if actual in number_str_count:
            number_str_count[actual] += 1
        else:
            number_str_count[actual] = 1
    # print(number_str_count)
    print('Подсчёт количества - ОК')

    print('Деление - START')
    # Делим общее время на количество записей (групп)
    number_str_new_time = {}
    for i, row in df.iterrows():
        actual = row['НомерСтроки']
        if actual not in number_str_new_time:
            number_str_new_time[actual] = row['Часы'] / number_str_count[actual]
    # # print(number_str_new_time)
    print('Деление - ОК')

    print('Запись в столбец - START')

    # # Создание потоков
    # PROC_COUNT = int(math.ceil(df.shape[0] / COUNT))
    # # print(df.shape[0])
    # # print(PROC_COUNT)
    # data_list = split_dataframe(df)
    # # print(len(data_list))
    #
    # manager = multiprocessing.Manager()
    # return_dict = manager.dict()
    #
    # prc = []
    #
    # i = 0
    # while i < PROC_COUNT:
    #     pr = multiprocessing.Process(target=write_in_column, args=(data_list[i], i, return_dict))
    #     prc.append(pr)
    #     pr.start()
    #     i += 1
    #
    # for i in prc:
    #     i.join()

    new_df = df.iloc[0:0]
    # Записываем данные в столбец
    for i, row in df.iterrows():
        # если убрать комментарций - данные не записываются. Не понимаю в чём причина
        # print(number_str_new_time[row['НомерСтроки']])
        row['Часы на группу'] = number_str_new_time[row['НомерСтроки']]
        new_df = pd.concat([pd.DataFrame([row.values], columns=df.columns), new_df], ignore_index=True)
    print('Запись в столбец - ОК')
    # # print(new_df.to_string())
    return new_df


def write_in_column(df, number_str_new_time):
    print('Запись в столбец - START')
    new_df = df.iloc[0:0]
    # Записываем данные в столбец
    for i, row in df.iterrows():
        # если убрать комментарций - данные не записываются. Не понимаю в чём причина
        # print(number_str_new_time[row['НомерСтроки']])
        row['Часы на группу'] = number_str_new_time[row['НомерСтроки']]
        new_df = pd.concat([pd.DataFrame([row.values], columns=df.columns), new_df], ignore_index=True)
    print('Запись в столбец - ОК')


def save_file(df, path):
    print('Запись в файл - START. Количество строк:', df.shape[0])
    df.to_excel(path)


def split_dataframe(df, chunk_size=COUNT):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size: (i + 1) * chunk_size])
    return chunks


def split_contingent_data(df, plan=4, group=0, student_count=5):
    print('Проверка плана - START')
    new_df2 = df.iloc[0:0]
    last_nonnull_row = None
    # перебор построчно
    for i, row in df.iterrows():
        # Проверяем значение на NaN
        if pd.isnull(row[group]):
            # У нас есть текущая строка (row) и строка которую мы запомнили (last_nonnull_row)
            # Сравниваем количество студентов
            # Если у текущей строки студентов больше, чем у последней не NaN строки (last_nonnull_row)
            # Тогда следует изменить данные в строке last_nonnull_row, в ином случае - пропускаем
            if row[student_count] > last_nonnull_row[student_count]:
                k = 0
                # Перебираем стобцы
                while k < df.shape[1]:
                    # Если значение столбца в row не является NaN, тогда записываем его в last_nonnull_row
                    # В ином случае пропускаем
                    if not pd.isnull(row[k]):
                        print('old: ', last_nonnull_row[k])
                        print('new: ', row[k])
                        last_nonnull_row[k] = row[k]
                    k += 1
            # else:
            #     print('|-----------------------------------------|')
            #     print('не подошла строка: ', row)
            #     print('на замену этой: ', last_nonnull_row)
            #     print('|-----------------------------------------|')
        else:
            # Если поле Group не Nan, значит можно сохранить строку, которая была записана на прошлой итерации
            if last_nonnull_row is not None:
                new_df2 = pd.concat([pd.DataFrame([last_nonnull_row.values], columns=df.columns), new_df2], ignore_index=True)
            # Затем запоминаем новую строчку
            last_nonnull_row = row

        # try:
        #     # Достаём строку пытаемся что-то изменить, если будет ошибка, значит там значение NaN
        #     row[group].replace(' ', ' ')
        #     # Если ошибка не вылетела значит можно сохранить строку, которая была записана на прошлой итерации
        #     if last_nonnull_row is not None:
        #         new_df2 = pd.concat([pd.DataFrame([row.values], columns=new_df.columns), new_df2], ignore_index=True)
        #     # Затем запоминаем новую строчку
        #     last_nonnull_row = row
        # except AttributeError:
        #     # У нас есть текущая строка и строка которую мы запомнили
        #     # Сравниваем количество студентов
        #     # Если у текущей строки студентов больше, чем у последней не NaN строки (last_nonnull_row)
        #     # Тогда следует изменить данные в строке last_nonnull_row
        #     if row[student_count] > last_nonnull_row[student_count]:
        #         if pd.isnull(row[1]):
        #             print(row)
    # Запись последней строки
    new_df2 = pd.concat([pd.DataFrame([last_nonnull_row.values], columns=df.columns), new_df2], ignore_index=True)
    print('Проверка плана - ОК')

    print('Разбиение рабочего плана - START')
    new_df = new_df2.iloc[0:0]
    # Перебираем таблицу построчно
    for i, row in new_df2.iterrows():
        try:
            # Убираем из строки "Рабочий план "
            data = row[plan].replace('Рабочий план ', '')
            # Находим вхождение подстроки " от " (индекс первого символа из подстроки)
            start = data.find(' от ')
            # Создаём новую строку и записываем все символы от 0 до индекса первого символа подстроки " от "
            result = data[0:start]
            row[plan] = result
            new_df = pd.concat([pd.DataFrame([row.values], columns=new_df2.columns), new_df], ignore_index=True)
        except AttributeError:
            new_df = pd.concat([pd.DataFrame([row.values], columns=new_df2.columns), new_df], ignore_index=True)
    print('Разбиение рабочего плана - ОК')

    save_file(new_df, 'resource/new.xlsx')
    return new_df


def exe():
    start_time = time.time()
    contingent_data = load_data_from_file(file='resource/контингент 2.xlsx', header=0)
    data = load_data_from_file(file='resource/нагрузка с потоками.xlsx', header=0)
    contingent = split_contingent_data(contingent_data, 4, 0, 5)

    PROC_COUNT = int(math.ceil(data.shape[0] / COUNT))
    # print(PROC_COUNT)
    data_list = split_dataframe(data)
    # print(len(data_list))
    # Создание потоков
    manager = multiprocessing.Manager()
    # Переменная куда будут записываться результаты после окончания потока
    return_dict = manager.dict()
    # Лист потоков
    prc = []

    # Создание и запуск потоков
    i = 0
    while i < PROC_COUNT:
        pr = multiprocessing.Process(target=split_thread, args=(data_list[i], i, return_dict))
        prc.append(pr)
        pr.start()
        i += 1

    # Дожидаемся завершения всех потоков
    for k in prc:
        k.join()

    # Общая таблица с результатами
    load_with_threads = data.iloc[0:0]
    # Перебор результатов и запись в общую таблицу
    for k in return_dict.values():
        load_with_threads = pd.concat([load_with_threads, k], ignore_index=True, axis=0)

    print(load_with_threads.shape[0])

    print("--- %s seconds ---" % (time.time() - start_time))

    # # print(contingent.head(3).to_string())
    # # print(load_with_threads.head(3).to_string())
    # Соединяем таблицы
    new_df = pd.merge(load_with_threads, contingent, on=['Группа', 'Группа'])
    # # print(new_df.head(40).to_string())
    # считаем время и сохраняем данные в файл
    save_file(calculate_time(new_df), 'resource/result.xlsx')
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    exe()
