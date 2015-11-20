# -*- coding: utf-8 -*-
import logging
import os.path
import time


#
# Utility funcions
#
def xstr(s):
    if s is None:
        return ''
    return str(s)


def isNumber(elem):
    try:
        float(elem)
        return True
    except (TypeError, ValueError):
        return False


def get_current_time():
    aux = str(time.strftime('%d/%m/%Y %H:%M:%S'))
    return aux    


def set_logging(log_folder, log_name):
    
    global logger 
    
    # Create logger
    logger = logging.getLogger('dbsync')
    logger.setLevel(logging.DEBUG)
    
    # Define filename and format
    tstamp = str(time.strftime('%Y%m%d'))
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)    
    filepath = log_folder+"/"+log_name+"_"+tstamp+".log"
    log_format = '%(asctime)s [%(levelname)s] - %(message)s\n'
    log_date = '%d/%m/%Y %H:%M:%S'
    formatter = logging.Formatter(log_format, log_date)
    
    # Create file handler
    fh = logging.FileHandler(filepath)
    fh.setLevel(logging.INFO)    
    fh.setFormatter(formatter)
    logger.addHandler(fh)    

    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)    

