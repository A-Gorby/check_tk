import os, sys
import zipfile
import requests
from urllib.parse import urlencode
import argparse
from g import logger

# from utils import unzip_file
def unzip_file(path_source, fn_zip, work_path):
    logger.info('Unzip ' + fn_zip + ' start...')

    try:
        with zipfile.ZipFile(path_source + fn_zip, 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logger.info('Unzip ' + fn_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logger.error('Unzip error: ' + str(err))
        sys.exit(2)

def upload_files(supp_dict_dir = '/content/data/supp_dict'):
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    # public_key = link #'https://yadi.sk/d/UJ8VMK2Y6bJH7A'  # Сюда вписываете вашу ссылку
    links = [('Коды МГФОМС и 804н.xlsx', 'https://disk.yandex.ru/i/lX1fVnK1J7_hfg', ('МГФОМС', '804н')),
    ('НВМИ_РМ.xls', 'https://disk.yandex.ru/i/_RotfMJ_cSfeOw', 'Sheet1'),
    ('МНН.xlsx', 'https://disk.yandex.ru/i/0rMKBimIKbS7ig', 'Sheet1'),
    ('df_mi_national_release_20230201_2023_02_06_1013.zip', 'https://disk.yandex.ru/d/pfgyT_zmcYrHBw' ),
    ('df_mi_org_gos_release_20230129_2023_02_07_1331.zip', 'https://disk.yandex.ru/d/Zh-5-FG4uJyLQg' ),
    # ('Специальность (унифицированный).xlsx', 'https://disk.yandex.ru/i/au5M0xyVDW2mtQ', None),
    ]

    # Получаем загрузочную ссылку
    for link_t in links:
        final_url = base_url + urlencode(dict(public_key=link_t[1]))
        response = requests.get(final_url)
        download_url = response.json()['href']

        # Загружаем файл и сохраняем его
        download_response = requests.get(download_url)
        # with open('downloaded_file.txt', 'wb') as f:   # Здесь укажите нужный путь к файлу
        with open(os.path.join(supp_dict_dir, link_t[0]), 'wb') as f:   # Здесь укажите нужный путь к файлу
            f.write(download_response.content)
            logger.info(f"File '{link_t[0]}' uploaded!")
            if link_t[0].split('.')[-1] == 'zip':
                fn_unzip = unzip_file(os.path.join(supp_dict_dir, link_t[0]), '', supp_dict_dir)
                logger.info(f"File '{fn_unzip}' upzipped!")

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--supp_dict_dir', '-dd', type=str, default = '/content/data/supp_dict',
        help="Directory for support dictionaries, default  '/content/data/supp_dict'")
            
    opt = parser.parse_args()
    return opt


if __name__ == '__main__':
    if len(sys.argv) > 1: # есть аргументы в командной строке
        opt = parse_opt()
        upload_files(**vars(opt))
    else:
        upload_files()