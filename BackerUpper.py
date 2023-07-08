from apscheduler.schedulers.background import BackgroundScheduler
import json
import requests
from datetime import datetime, timedelta
import os
from tr01_config import ConfigDev, ConfigProd, ConfigLocal
import logging
from logging.handlers import RotatingFileHandler
# import pandas as pd


if os.environ.get('FLASK_CONFIG_TYPE')=='local':
    config = ConfigLocal()
elif os.environ.get('FLASK_CONFIG_TYPE')=='dev':
    config = ConfigDev()
elif os.environ.get('FLASK_CONFIG_TYPE')=='prod':
    config = ConfigProd()



#Setting up Logger
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

#initialize a logger
logger_main = logging.getLogger(__name__)
logger_main.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
if not os.path.exists(os.path.join(config.BACKER_UPPER_ROOT,'logs')):
    os.makedirs(os.path.join(config.BACKER_UPPER_ROOT,'logs'))
file_handler = RotatingFileHandler(os.path.join(config.BACKER_UPPER_ROOT,'logs','BackerUpper.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_main.addHandler(file_handler)
logger_main.addHandler(stream_handler)


def scheduler_func():
    logger_main.info(f"- start TuRincon01backerUpper scheduler -")
    logger_main.info(f"- using api base url: {config.API_URL}  -")

    scheduler = BackgroundScheduler()

    # job_01 = scheduler.add_job(daily_backup, 'cron', minute='*', second='05')
    # job02_call_api_move_tr_backup01 = scheduler.add_job(call_api_move_tr_backup01, 'cron', day="*", hour="00")
    # job02_call_api_create_tr_backup01 = scheduler.add_job(call_api_create_tr_backup01, 'cron', day="*", hour="00")
    test_job02_call_api_create_tr_backup01 = scheduler.add_job(call_api_create_tr_backup01, 'cron', minute="30", second="05")
    # job_01 = scheduler.add_job(daily_backup, 'cron', minute='*', second='05')

    scheduler.start()

    while True:
        pass


def daily_backup():# Testing (i.e NOT used)

    logger_main.info(f"- in daily_backup -")
    logger_main.info(f"- find database: {config.SQL_URI} -")

    base_url = config.API_URL#TODO: put this address in config
    headers = { 'Content-Type': 'application/json'}
    payload = {}
    payload['TR_VERIFICATION_PASSWORD'] = config.TR_VERIFICATION_PASSWORD
    response_tr_backup = requests.request('POST',base_url + '/download_tr_backup',
        headers=headers, data=str(json.dumps(payload)))


    logger_main.info(f"- response: {response_tr_backup.status_code} -")
    if response_tr_backup.status_code == 200:
        logger_main.info(f"- response: {response_tr_backup.json} -")

    # logger_main.info(f"- zip file with backup date -")
    # logger_main.info(f"- save in backups -")
    logger_main.info(f"- Done. {config.BACKUP_ROOT} -")


def call_api_move_tr_backup01():
    logger_main.info(f"- in call_api_move_tr_backup01")

    headers = { 'Content-Type': 'application/json'}
    payload = {'TR_VERIFICATION_PASSWORD':config.TR_VERIFICATION_PASSWORD}
    response_move_backup = requests.request('POST',config.API_URL + '/move_tr_backup01',
        headers=headers, data=str(json.dumps(payload)))
    
    if response_move_backup.status_code == 200:
        logger_main.info(f"Successful api response ({response_move_backup.status_code}): {response_move_backup.json().get('status')}")
        
        

    elif response_move_backup.status_code == 500:
        logger_main.info(f"Unsuccessful api response ({response_move_backup.status_code})")
        try:
            logger_main.info(f"Unsuccessful api response message: {response_move_backup.headers.get('message')})")
        except:
            logger_main.info("Unknown 500 message")
    else:
        logger_main.info(f"Unsuccessful api response ({response_move_backup.status_code})")
    
    logger_main.info("- Done: daily backup cycle finished -")
    

def call_api_create_tr_backup01():

    logger_main.info(f"- in call_api_create_tr_backup01")

    headers = { 'Content-Type': 'application/json'}
    payload = {'TR_VERIFICATION_PASSWORD':config.TR_VERIFICATION_PASSWORD}
    response_create_backup = requests.request('POST',config.API_URL + '/create_tr_backup01',
        headers=headers, data=str(json.dumps(payload)))

    if response_create_backup.status_code == 200:
        logger_main.info(f"Successful api response ({response_create_backup.status_code}): {response_create_backup.json().get('status')}")
        call_api_move_tr_backup01()
    else:
        logger_main.info(f"Unsuccessful api response ({response_create_backup.status_code})")



if __name__ == '__main__':
    scheduler_func()