import logging
df_services_MGFOMS, df_services_804n, df_RM, df_MNN = None, None, None, None

# logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('TKBD')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
strfmt = '[%(asctime)s] [%(name)s] [%(levelname)s] > %(message)s'
strfmt = '%(asctime)s - %(levelname)s > %(message)s'
# строка формата времени
datefmt = '%Y-%m-%d %H:%M:%S'
datefmt = '%H:%M:%S'
# создаем форматтер
formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
