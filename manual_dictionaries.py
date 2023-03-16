import numpy as np
gt_cols_chunks = [
['Наименование ТК',
 '№ п/п',
 'Код услуги по Номенклатуре медицинских услуг (Приказ МЗ № 804н)',
 'Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ №804н)',
 'Код услуги по Реестру МГФОМС',
 'Усредненая \nчастота \nпредоставления',
 'Усредненная \nкратность \nприменения',
 'УЕТ 1',
 'УЕТ 2',
 'Комментарии',
 'Сумма УЕТ=0(услуга отсутствует) (1-да, 0-нет)',
 'ФИО ГВС'],
['Наименование ТК',
 '№ п/п',
 'Наименование лекарственного препарата (ЛП) (МНН)',
 'Код группы ЛП (АТХ)',
 'Форма выпуска лекарственного препарата (ЛП)',
 'Усредненая \nчастота \nпредоставления',
 'Усредненная \nкратность \nприменения',
 'Единицы измерения',
 'Кол-во',
 'Комментарии',
 'Цена за единицу, руб.',
 'Итого расход, руб.',
 'Наличие препарата в ЖНВЛП (1 - есть; 0 - нет)',
 'ФИО ГВС'],
['Наименование ТК',
 '№ п/п',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги',
 'Код МИ из справочника (на основе утвержденного Перечня НВМИ)',
 'Усредненная частота предоставления',
 'Усредненная кратность применения',
 'Ед. измерения',
 'Кол-во',
 'Комментарии',
 'Цена за единицу, руб.',
 'Итого расход, руб.',
 'Есть данные по стоимости?',
 'ФИО ГВС'],
]

data_chunks = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты, обязательно используемые при оказании медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги',
 'Наименование (вариант) диеты',
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]
data_chunks_alter = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты, обязательно используемые при оказании комплексной медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании комплексной медицинской услуги',
 'Лечебное питание',
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]
data_chunks_alter_02 = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты , обязательно используемые при оказании медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании услуги',
 'Лечебное питание',
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]


# data_chunks = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
#  'Лекарственные препараты, обязательно используемые при оказании медицинской услуги',
#  'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги',
#  'Наименование (вариант) диеты'
# ]
# data_chunks_alter = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
#  'Лекарственные препараты, обязательно используемые при оказании комплексной медицинской услуги',
#  'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании комплексной медицинской услуги',
#  'Лечебное питание']
# data_chunks_alter_02 = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
#  'Лекарственные препараты , обязательно используемые при оказании медицинской услуги',
#  'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании услуги',
#  'Лечебное питание']
# kw_head_cols = [
# # chunk 0 - serv
#    ['№', 'п/п'] ,
#    [],

# ]
# new version
data_chunks = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты, обязательно используемые при оказании медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги',
 'Наименование (вариант) диеты',
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]
data_chunks_alter = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты, обязательно используемые при оказании комплексной медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании комплексной медицинской услуги',
 'Лечебное питание',
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]
data_chunks_alter_02 = ['Перечень медицинских услуг, используемых при оказании комплексной медицинской услуги',
 'Лекарственные препараты , обязательно используемые при оказании медицинской услуги',
 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании услуги',
 'ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ',
 # '5.Лечебное питание'
 # 'Наименование (вариант диеты)Согласно Приказа МЗ РФ  от 21.06.2013 №395н'
 'Основные требования к помещению, в котором оказывается медициеская услуга, в соответствии с установленными СНиП и СанПин',
 'Перечень оборудования, необходимого для оказания медицинской услуги',
]

# new vrsion
cols_chunks_02 = [
[
 ['№ п/п'],
 ['Код услуги по Номенклатуре медицинских услуг (Приказ МЗ № 804н)', 
  # 'Код услуги по Номенклатуре медицинских услуг (Приказ МЗ от № 804н)',
  'Код услуги по Номенклатуре медицинских услуг (приказ МЗ от № 804н)',
  'Код услуги по Приказу МЗиСР №804н',
  'Код услуги по Номенклатуре медицинских услуг (Приказ МЗ РФ №804н)',
 # 'Код услуги по Номенклатуре медицинских услуг \n(Приказ МЗ от № 804н)'
 ],
 ['Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ №804н)',
  # 'Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ № 804н)',
  'Наименование услуги по Номенклатуре медицинских услуг (приказ МЗ № 804н)',
  'Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ РФ №804н)',
  # 'Наименование',
 ],
 ['Код услуги по Реестру МГФОМС', 'Код услуги ОМС',
 # опечатки
  'Код услуги по Реестру МГОФМС'],
 ['Усредненная частота\nпредоставления', 'Частота предоставления', 'Усредненная частота применения',
 # опечатки
  'Усредненная частота педоставления',
 ],
 ['Усредненная кратность применения', 'Кратность применения'],
 ['УЕТ 1','Время (мин.) / УЕТ 1'],
 ['УЕТ 2', 'Время (мин.) / УЕТ 2'],
],

[
 ['№ п/п'],
 ['Наименование лекарственного препарата', 'Наименование лекарственного препарата (ЛП)', 'Наименование лекарственного препарата (ЛП) (МНН)',
 'Наименование лекарственного средства (ЛС)',
 # 'наименование лекарственного средства (ЛС)',
 ], 
 ['Код ЛП и МНН из справочника (на основе утвержденного\nПеречня ЖНВЛП)','Код группы ЛП (АТХ)',
 'Код ЛС и МНН из справочника (на основе утвержденного Перечня ЖНВЛП)',
 ],
 ['Форма выпуска лекарственного препарата (ЛП)', 'Форма выпуска ЛС',
 'Форма выпуска лекарственных препаратов(ЛП)'  ,
 'Форма выпуска лекарственного препарата'],
 ['Усредненная частота\nпредоставления', 'Частота предоставления', 'Усредненная частота применения',
  # опечатки
  'Усредненная частота педоставления',
 ], 
 ['Усредненная кратность применения', 'Кратность применения'], 
 ['Ед. измерения','Ед, измерения', 'Единицы измерения', 'Единица измерения', #'ед. измерения'
 ],
 ['Кол-во', 'Кол-во (на 1 применение)', 'Кол-во на курс (среднекурсовая дозировка)', 
  'Кол-во на курс лечения', 'Кол-во на курс', 'Среднекурсовая доза',
 # 'Кол-во на курс лечения (среднекурсовая дозировка)', 
  'Кол-во на законченный случай', 'Кол-во на случай', 'Количество',
 '(средняя курсовая доза)', '(средняя суточная доза)', 'Кол-во (ССД)', 'Количество на курс (ССД)']
],

[['№ п/п'],
 ['Наименование медицинского изделия, расходных материалов (МИ)', 
  'Наименование медицинских изделий, расходных материалов (МИ)',
  'Наименование медицинского изделия, инструмента (МИ)',
 ],
 ['Код МИ из справочника (на основе утвержденного Перечня НВМИ)',
 # опечатки
  'Код МИ  из справочника   (на основе утвержденногоПеречня НВМИ)'],
 ['Усредненная частота\nпредоставления', 'Частота предоставления', 'Усредненная частота применения',
 # опечатки
  'Усредненная частота педоставления',
 ],
 ['Усредненная кратность применения', 'Кратность применения'],
 ['Ед. измерения', 'Ед, измерения', 'Единицы измерения', 'Единица измерения',] ,# 'ед. измерения'],
 ['Кол-во', 'Кол-во (на 1 применение)', 'Кол-во на курс (среднекурсовая дозировка)', 
  'Кол-во на курс лечения', 'Кол-во на курс', 'Среднекурсовая доза',
 # 'Кол-во на курс лечения (среднекурсовая дозировка)',
  'Кол-во на законченный случай', 'Кол-во на случай', 'Количество',
 '(средняя курсовая доза)','(средняя суточная доза)', 'Кол-во (ССД)', 'Количество на курс (ССД)']
]
]




main_cols = [(1,3), (1,3), (1,2)]
dtypes_chunks = [
# [np.str_, np.str_, np.str_, np.str_, np.str_, np.float64, np.float64, np.float64 ],
[np.str_, np.str_, np.str_, np.str_, object, object, object, object ],    
[np.str_, np.str_, np.str_, np.str_, object, object, np.str_, object ],
# [np.str_, np.str_, np.str_, np.float64, np.float64 , np.str_, np.float64  ]
[np.str_, np.str_, np.str_, object, object, np.str_, object  ]
]
# dtypes_chunks_dict
dtypes_chunks_dicts = [dict(zip(gt_cols_chunks[j][1:],dtypes_chunks[j])) for j, _ in enumerate(dtypes_chunks)]
print(dtypes_chunks_dicts); print()
dtypes_chunks_after_dict = [ {k: np.float64 for k,v in dtypes_chunk_dict.items() if v == object } 
                for dtypes_chunk_dict in dtypes_chunks_dicts ]
# print(dtypes_chunks_after_dict)

err_msg_lst = [
[['Не заполнено поле "№ п/п"'],
["Не заполнено поле 'Код услуги по приказу 804н'",
 "'Код услуги по приказу 804н' отсутствует в справочнике",
 "В поле 'Код услуги по приказу 804н' ошибочно указаны русские буквы",
 ],
 ["Не заполнено поле 'Наименование услуги по приказу 804н'",
  "'Наименование услуги по приказу 804н' отсутствует в справочнике",
  "В названии услуги содержитя ошибочный символ переноса строки",
 "Указанная связка 'Код услуги по приказу 804н' и 'Наименование услуги' отсутствует в справочнике",],
 ["Не заполнено поле 'Код МГФОМС'",
 "'Код МГФОМС' отсутствует в справочнике",
  
 ], 
 # 'Не соответствие код МГФОМС и наименование услуги / код записан некорректно (русскими буквами)'],
 ['Не заполнено поле "Частота"',
  "В поле 'Частота' стоит '.', а не ','",
  'Частота ошибочно > 1 или <= 0 или имеет недопустимый формат числа',
 "В поле 'Частота' недопустимый формат числа",],
 ["Поле 'Кратность' не заполено" , "В поле 'Кратность' стоит '.', а не ','",
  'Кратность услуг ошибочно не является целым числом или имеет недопустимый формат числа',
 "В поле 'Кратность' недопустимый формат числа",],
 ["Не заполнено поле 'УЕТ 1'",
 "Связка 'УЕТ 1' и 'Код МГФОМС' отсутствует в справочнике"],
["Не заполнено поле  'УЕТ 2'",
  "Связка 'УЕТ 2' и 'Код МГФОМС' отсутствует в справочнике"],],
[['Не заполнено поле "№ п/п"'],
 ["Не заполнено поле 'МНН'", "'МНН' отсутствует в справочнике"],
 ["Не заполнено поле 'Код АТХ'", "В поле 'АТХ' ошибочно указаны русские буквы", ],
 ["Не заполнено поле 'Форма выпуска ЛП'"],
 ["Не заполнено поле 'Частота'",  "В поле 'Частота' стоит '.', а не ','",
  'Частота > 1 или <= 0 или имеет недопустимый формат числа', "В поле 'Частота' недопустимый формат числа",],
 ["Не заполнено поле 'Кратность'",  "В поле 'Кратность' стоит '.', а не ','",
 "Кратность ошибочно < 1 или имеет недопустимый формат числа", 
  "В поле 'Кратность' недопустимый формат числа",],
 ["Не заполнено поле 'Единица измерения'",], # 'Ошибка в записе единиц измерения',],
 ["Не заполнено поле 'Количество'", "В поле 'Количество' стоит '.', а не ','",
 "В поле 'Количество' недопустимый формат числа",],
 ], 
# 'Ошибка в значении частоты (больше 1 или 0)',
#  'Ошибка в значении кратности (больше 1)',
#  'Ошибка в значении частоты (найдена "." в значении)',
#  'Ошибка в значении кратности (найдена "." в значении)',
#  'Ошибка в записе единиц измерения',
#  'Ошибка в сопоставлении код НВМИ + название РМ'
    
[['Не заполнено поле "№ п/п"'],
 ['Не заполнено поле "Наименование МИ/РМ"', "'МИ' отсутствует в справочнике"],
 ['Не заполнено поле "Код МИ/РМ"', "'Код МИ' отсутствует в справочнике"],
 ["Не заполнено поле 'Частота'", "В поле 'Частота' стоит '.', а не ','", 
 'Частота > 1 или <= 0 или имеет недопустимый формат числа',
  "В поле 'Частота' недопустимый формат числа",],
 ["Не заполнено поле 'Кратность'",  "В поле 'Кратность' стоит '.', а не ','",
 # "Кратность ошибочно < 1 или имеет недопустимый формат числа", 
  "Кратность ошибочно > 1 или имеет недопустимый формат числа", 
  "В поле 'Кратность' недопустимый формат числа",],
  ["Не заполнено поле 'Единица измерения'",], 
 ["Не заполнено поле 'Количество'", "В поле 'Количество' стоит '.', а не ','",
 "В поле 'Количество' недопустимый формат числа",],
]
]