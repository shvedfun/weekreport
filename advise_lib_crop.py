import datetime
import dateutil
import math
import pandas as pd
import numpy as np
#from sklearn.linear_model import LinearRegression
#from hdbcli import dbapi
import json
#import adviser_new as ad
#import joblib
import traceback
import os, glob
import time

VALIDATE_LINE = 0

convert_param_name = {'ANALOG\WFIR21_1\PV':'503_W21_1', 'ANALOG\FIRC841_1\PV':'503_F841_1', 'ANALOG\PIRC4463_1\PV':'503_P4463_1',
                      'ANALOG\FIRC17_1\PV':'503_F17_1', 'ANALOG\PIR16_1\PV':'503_P16_1', 'ANALOG\TIR812_1\PV':'503_T812_1',
                      'ANALOG\TIR811_1\PV':'503_T811_1', 'ANALOG\FIRC14_1\PV':'503_F14_1', 'ANALOG\EIR10_1\PV':'503_E10_1',
                      'ANALOG\PIR15_1\PV':'503_P15_1', 'ANALOG\PIR18_1\PV':'503_P18_1'}
adv_param_name = {'ADV\PIRC4463_1\PV':'ANALOG\PIRC4463_1\PV', 'ADV\TIR812_1\PV':'ANALOG\TIR812_1\PV', 'ADV\PIR16_1\PV':'ANALOG\PIR16_1\PV'}
convert_adv_param_name = {'ADV\PIRC4463_1\PV':'503_P4463_1', 'ADV\TIR812_1\PV':'503_T812_1', 'ADV\PIR16_1\PV':'503_P16_1'}

class CacheData():
    def __init__(self, param_validate=None, data=None):
        self.param_validate = param_validate
        self.data = data

    def validate(self, param_list):
        i = 0
        if len(param_list) != len(self.param_validate):
            return False
        for param in param_list:
            if param != self.param_validate[i]:
                return False
            i += 1
        return True

    def get_data(self):
        return self.data

def connect_2_db_Cloud():
    """
    Устанавливает соединение
    """
    conn = dbapi.connect(
        address="zeus.hana.prod.eu-central-1.whitney.dbaas.ondemand.com",
        port=21824,
        user="vshvedov",
        password="Hana12345",
        encrypt="true",
        sslCryptoProvider="openssl",
        sslTrustStore="-----BEGIN CERTIFICATE-----\nMIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh\nMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3\nd3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD\nQTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT\nMRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j\nb20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG\n9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB\nCSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97\nnh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt\n43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P\nT19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4\ngdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO\nBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR\nTLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw\nDQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr\nhMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg\n06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF\nPnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls\nYSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk\nCAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=\n-----END CERTIFICATE-----\n",
        currentschema="uralchem")
    conn.setautocommit(False)
    return conn

def connect_2_db_old():
    """
    Устанавливает соединение
    """
    conn = dbapi.connect(
        address="b30srv202.uc.local",
        port=30015,
        user="REPORT_TECH",
        password="1qaz2WSX",
        currentschema="uralchem")
    conn.setautocommit(False)
    return conn

def connect_2_db(skip_check = False, set_error = False):
    f_conn = False
    address = "b30srv202.uc.local"
    if set_error:
        address = "errurl.uc.local"
    while(not f_conn):
        try:
            conn = dbapi.connect(
                address= address, #"b30srv202.uc.local"
                port=30015,
                user="REPORT_TECH",
                password="1qaz2WSX",
                currentschema="uralchem")
            f_conn = conn.isconnected()
            if skip_check:
                f_conn = True
            if not f_conn:
                conn.close()
                print(f'Cant connect to DB: {address}')
                time.sleep(30)
        except:
            print(f'Exception connect to DB: {address}')
            time.sleep(1)
    conn.setautocommit(False)
    return conn


def get_raw_all_bgs(cursor, start_time, dumpName='raw_data.dump')->pd.DataFrame:
    """
    Читает сырые данные из файла dumpName и добавляет к ним новые сырые данные из БД
    :param cursor:
    :param start_time: время начала данные
    :param dumpName: имя файла
    :return:
    """
    f_exsts = os.path.isfile(dumpName)
    if f_exsts:
        c_data: CacheData = joblib.load(dumpName)
        #print(f'Raw_data. Данные взял из кэша')
        df = c_data.get_data()
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        start_time = df['TIMESTAMP'].max()
        df = df[df['TIMESTAMP']<start_time]

    else:
        df = pd.DataFrame()

    print(f'start_time = {start_time}')
    step = 4
    finish = start_time+datetime.timedelta(hours=step)
    while start_time<=datetime.datetime.utcnow():
        try:
            res = cursor.execute('SELECT "ID_RAW_DATA","TIMESTAMP","BGS_ID","PARAM_NAME","PARAM_VALUE","ID_PROCESSED_DATA","SOURCE_NAME" FROM "URALCHEM"."RAW_DATA" where "TIMESTAMP">=:start_time AND "TIMESTAMP"<:finish ORDER BY "TIMESTAMP"',{'start_time': start_time,'finish':finish})
            res = cursor.fetchall()
            res = pd.DataFrame(res, columns=["ID_RAW_DATA","TIMESTAMP","BGS_ID","PARAM_NAME","PARAM_VALUE","ID_PROCESSED_DATA","SOURCE_NAME"])
            df = df.append(res,ignore_index=True)
            #print(f'Прочитал на время = {finish}')
            start_time = finish
            finish+=datetime.timedelta(hours=step)
        except Exception as inst:
            print("----------------ERROR")
            print(f'type = {type(inst)}, inst.args = {inst.args}, Exception = {inst}')
            traceback.print_exc()

    df.sort_values('TIMESTAMP', inplace=True)
    df.reset_index(drop=True,inplace=True)
    c_data = CacheData(('1'), df)
    joblib.dump(c_data,dumpName)
    return df

def parse_adv(adv, adv_param_name):
    """
    Парсит рекомендации из adv по adv_param_name
    :param adv:
    :param adv_param_name:
    :return:
    """
    adv_df = pd.DataFrame()
    for index, row in adv.iterrows():
        try:
            #print(f'index={index}, row={row["TEXT2"]}')
            if len(row["TEXT2"])>0:
                res=json.loads(row['TEXT2'])
                #print(f'res = {res}')
                if len(res)>0:
                    res['tst_save'] = row['TIMESTAMP']
                    adv_df = adv_df.append(res, ignore_index=True)
                #print(res)
        except:
            print(f'len(row["TEXT2"]) = {len(row["TEXT2"])}')
            print(f'Error decode json = {row["TIMESTAMP"]} ')

    if adv_df.shape[0]==0:
        return adv_df
    adv_df['start'] = pd.to_datetime(adv_df['start']) #+ pd.Timedelta(hours=3)
    #print(f'adv_df.columns = {adv_df.columns}')
    res_adv_df = pd.DataFrame()
    for key, value in adv_param_name.items():
        # print(value)
        df: pd.DataFrame = adv_df[adv_df['parametr'] == value]
        #df.drop_duplicates('start', inplace=True)
        df=df.sort_values('start')
        df.reset_index(drop=True, inplace=True)
        #df['-1'] = (df.shift(1)['advise_val'] - df.shift(1)['current_val']) + df.shift(2)['advise_val']
        #df['-2'] = (df['advise_val'].shift(1) - df['current_val'].shift(1)) + (
        #            df['advise_val'].shift(2) - df['current_val'].shift(2)) + df['advise_val'].shift(3)
        #df['-3'] = (df['advise_val'].shift(1) - df['current_val'].shift(1)) + (
        #            df['advise_val'].shift(2) - df['current_val'].shift(2)) + (
        #                       df['advise_val'].shift(3) - df['current_val'].shift(3)) + df['advise_val'].shift(4)
        res_adv_df = res_adv_df.append(df, ignore_index=False)
    res_adv_df.sort_values('start', inplace=True)
    res_adv_df.reset_index(drop=True, inplace=True)
    #res_adv_df['delta-3'] = res_adv_df['-3'] - res_adv_df['current_val']
    #res_adv_df['delta_pred'] = res_adv_df['pred_adv_val'] - res_adv_df['advise_val']
    #res_adv_df['delta_half'] = res_adv_df['half_adv_val'] - res_adv_df['advise_val']
    #res_adv_df = res_adv_df[['start', 'parametr', 'current_val', 'advise_val', 'retur_speed', 'retur_accel', 'curr_retur', 'target_retur', 'direction','2step_speed', '2step_accel', 'tst_save']] #, '-1', '-2', '-3', 'delta-3'
    #, 'pred_adv_val', 'pred_speed', 'pred_accel', 'pred_direction', 'half_speed', 'half_accel', 'half_adv_val', 'half_direction', 'delta_pred', 'delta_half'
    return res_adv_df

def get_advises(cursor, start_time, bgs_id = 1, type_pred = 2, modul ='Predict')->pd.DataFrame:
    """
    Возвращает из БД значения прогнозов УП. Важно, УП!!!
    :param cursor:
    :param start_time:
    :param bgs_id:
    :param modul:
    :return:
    """
    res = cursor.execute('SELECT "TIMESTAMP_PREDICT", "PARAM_NAME", "PARAM_VALUES" FROM "URALCHEM"."PREDICT_DATA_LINES" WHERE "ID_PREDICT_DATA_HEAD" IN (SELECT "ID_PREDICT_DATA_HEAD" FROM "URALCHEM"."PREDICT_DATA_HEADERS" WHERE "TYPE_PREDICT"=:type_pred AND "BGS_ID"=:bgs_id AND "TIMESTAMP">=:start_time) ORDER BY "TIMESTAMP_PREDICT"',{'start_time': start_time, 'bgs_id':bgs_id, 'type_pred':type_pred})
    res = cursor.fetchall()
    res = pd.DataFrame(res, columns=['newtime', 'param_name', 'value'])
    return res


def get_advise_value_from_status(cursor, start_time, modul ='Predict', err_code=0, bgs_id = 1, end_time = None)->pd.DataFrame:
    if end_time is None:
        end_time = datetime.datetime.utcnow()
    res = cursor.execute('SELECT "MODUL","TIMESTAMP","OPERATION","STATUS","ERROR_CODE","ERROR_TEXT","TEXT2" FROM "URALCHEM"."STATUS" where "TIMESTAMP">=:start_time AND "TIMESTAMP"<:end_time and "MODUL"=:modul and "ERROR_CODE"=:err_code and "BGS_ID"=:bgs_id ORDER BY "TIMESTAMP"',{'start_time': start_time,'end_time':end_time,'modul': modul, 'err_code':err_code, 'bgs_id':bgs_id})
    res = cursor.fetchall()
    res = pd.DataFrame(res, columns=["MODUL","TIMESTAMP","OPERATION","STATUS","ERROR_CODE","ERROR_TEXT","TEXT2"])
    return res

def get_fact_time(cursor, start_time, bgs_id=1, end_time = None)->pd.DataFrame:
    if end_time is None:
        end_time = datetime.datetime.utcnow()
    res = cursor.execute('SELECT DISTINCT "TIMESTAMP" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "TIMESTAMP">=:start_time AND "TIMESTAMP"<:end_time AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"',
        {"bgs_id": bgs_id, 'start_time': start_time, 'end_time': end_time, 'validate_status':VALIDATE_LINE})
    fact = cursor.fetchall()
    fact = pd.DataFrame(fact,columns=['newtime'])
    return fact


def get_fact_value(cursor, param_name, history_name, start_time, bgs_id=1, end_time = None)->pd.DataFrame:
    if end_time is None:
        end_time = datetime.datetime.utcnow()
    if param_name is not None:
        res = cursor.execute('SELECT "TIMESTAMP", "PARAM_VALUE" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "TIMESTAMP">=:start_time AND "TIMESTAMP"<:end_time AND "PARAM_NAME"=:param_name AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"',
            {"bgs_id": bgs_id, 'start_time': start_time, 'param_name':param_name, 'end_time': end_time, 'validate_status':VALIDATE_LINE})
    else:
        res = cursor.execute('SELECT "TIMESTAMP", "PARAM_NAME", "PARAM_VALUE" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "TIMESTAMP">=:start_time AND "TIMESTAMP"<:end_time AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"',
            {"bgs_id": bgs_id, 'start_time': start_time, 'end_time': end_time, 'validate_status':VALIDATE_LINE})
    fact = cursor.fetchall()
    if param_name is not None:
        fact = pd.DataFrame(fact,columns=['newtime',history_name])
    else:
        fact = pd.DataFrame(fact,columns=['newtime','param_name','param_value'])
    return fact

def get_pred_value(cursor, head_id, param_name, history_name)->pd.DataFrame:
    #print(f'param_name = {param_name}, history_name = {history_name}')
    res = cursor.execute('SELECT TIMESTAMP_PREDICT, PARAM_VALUES FROM PREDICT_DATA_LINES WHERE ID_PREDICT_DATA_HEAD=:head_id AND "PARAM_NAME"=:param_name ORDER BY TIMESTAMP_PREDICT',
    {'head_id': head_id, 'param_name':param_name})
    pred = cursor.fetchall()
    pred = pd.DataFrame(pred,columns=['newtime',history_name])
    return pred

def read_fact_advise_2_df(cursor, convert_param_name, convert_adv_param_name, bgs_id, start_time, finish_time=None,
                          type_predict=2, step=60, radius=10):
    """
    Читаем фактические и предсказанные данные из БД
    :param finish_time:
    :param cursor:
    :param convert_param_name:
    :param convert_adv_param_name:
    :param start_time:
    :param type_predict:
    :return:
    """
    if finish_time is None:
        finish_time = datetime.datetime.utcnow()
    max_time = datetime.datetime(year=2019, month=1, day=1, hour=1)
    res_df = pd.DataFrame()
    for name, db_name in convert_param_name.items():
        #print(f'name = {name}')
        fact = get_fact_value(cursor,db_name, name, start_time, bgs_id=bgs_id, end_time=finish_time)
        if len(fact) > 0:
            res_df['newtime'] = fact['newtime']
            res_df[name] = fact[name]
        max_tmp = fact.loc[:, 'newtime'].max()
        max_tmp = pd.to_datetime(max_tmp)
        #max_tmp = max_tmp.to_datetime()
        #print(f'max_tmp = {max_tmp}, type = {type(max_tmp)}')
        if max_tmp > max_time:
            max_time = max_tmp
    #print(f'res_df.shape = {res_df.shape}')
    #res_df.sort_values('newtime',inplace=True)
    res_df = res_df.set_index('newtime', drop=False)
    res_df = res_df.sort_index()
    res = cursor.execute('SELECT ID_PREDICT_DATA_HEAD, TYPE_PREDICT, TIMESTAMP FROM PREDICT_DATA_HEADERS WHERE TIMESTAMP>=:start_tme AND TIMESTAMP<:finish_tme AND TYPE_PREDICT =:type_predict AND BGS_ID=bgs_id',{'start_tme':start_time,'finish_tme':finish_time,'type_predict':type_predict, 'bgs_id':bgs_id}) # OR TYPE_PREDICT = :type_predict1
    list_pred_head = cursor.fetchall()
    list_pred_head = pd.DataFrame(list_pred_head, columns=['id_head', 'type_pred','newtime'])
    list_pred_head = list_pred_head.head(list_pred_head.shape[0]-1)
    print(list_pred_head.shape)
    pred_df = pd.DataFrame()
    for index,pred_head in list_pred_head.iterrows():
        head_id = pred_head[0]
        type_pred = pred_head[1]
        time_stamp = pred_head[2]
        if index%200==0:
            print(f'Обрабатываю {index} из {list_pred_head.shape[0]}. Время {time_stamp}')
        for name, db_name in convert_adv_param_name.items():
            #print(f'name = {name}, db_name = {db_name}')
            pred = get_pred_value(cursor, head_id, db_name, name)
            pred['head_id'] = head_id
            pred['head_tst'] = time_stamp
            #print(f'pred = {pred}')
            if len(pred)==0:
                continue
            #print(f'min = {pred.index.min()}, max = {pred.index.max()}')
            pred = pred.iloc[step-radius-1:step+radius-1,:]
            #for i in range(1, step):
            #    val = pred.tail(1)[name].values[0]
            #    new_time = pred.tail(1)['newtime'].values[0]+pd.Timedelta(minutes=1)
            #    pred=pred.append({'newtime':new_time,name:val},ignore_index=True) #,'head_id':head_id, 'head_tst':time_stamp
            #pred.sort_values('newtime', inplace=True)
            pred = pred.set_index('newtime',drop=False)
            pred = pred.sort_index()
            min_v = pred.index.min()
            max_v = pred.index.max()
            #print(name)
            if pred[name].count() == res_df.loc[min_v:max_v].count()[0]:
                res_df.loc[min_v:max_v,name] = pred[name].values
                res_df.loc[min_v:max_v,'head_id'] = pred['head_id'].values
                res_df.loc[min_v:max_v,'head_tst'] = pred['head_tst'].values
            else:
                pass
                print(f'Не добавляю {name}, min_v = {min_v}, max_v = {max_v}')
            #print(pred)

    res_df.reset_index(drop=True, inplace=True)
    return res_df


def get_fact_df(cursor, convert_param_name, start_time, bgs_id, dropna = True, end_time = None):
    if end_time is None:
        end_time = datetime.datetime.utcnow()
    max_time = datetime.datetime(year=2019, month=1, day=1, hour=1)
    res_df = pd.DataFrame()
    fact = get_fact_time(cursor, start_time, bgs_id, end_time)
    res_df['newtime'] = fact['newtime'].unique()
    res_df.sort_values('newtime',inplace=True)
    res_df.set_index('newtime',drop=False,inplace=True)
    end_time = res_df['newtime'].max()
    #exit(77)
    for name, value in convert_param_name.items():
        print(f'Read parameter = {name}')
        #fact = cf.get_fact_value(cursor,value, name, start_time)
        #fact_loc = fact[fact['param_name']==value]
        fact_loc = get_fact_value(cursor, value, name, start_time, bgs_id, end_time)
        print(f'fact_loc.shape = {fact_loc.shape}, fact_loc.columns = {fact_loc.columns}')
        #if fact_loc.shape[0]!=fact_loc['newtime'].unique().shape[0]:
        #    tst = fact_loc[fact_loc['newtime']==fact_loc.shift(1)['newtime']]
            #print(tst)
            #print(f'Есть дублирование!!!, {fact_loc.shape[0]} != {fact_loc["newtime"].unique().shape[0]}')
        fact_loc = fact_loc.drop_duplicates('newtime',inplace=False)
        fact_loc = fact_loc.sort_values('newtime',inplace=False)
        fact_loc.set_index('newtime',drop=False,inplace=True)
        if fact_loc.shape[0] > 0:
            res_df[name] = fact_loc[name]
        max_tmp = fact_loc['newtime'].max()
        max_tmp = pd.to_datetime(max_tmp)
        #max_tmp = max_tmp.to_datetime()
        #print(f'max_tmp = {max_tmp}, type = {type(max_tmp)}')
        if max_tmp > max_time:
            max_time = max_tmp
    #res_df.sort_values('newtime',inplace=True)
    if dropna:
        res_df.dropna(inplace=True)
    return res_df

def add_predict(res_df, cursor, convert_adv_param_name, start_time, type_predict, start, length, skip_count = 1, end_time = None):
    if end_time is None:
        end_time = datetime.datetime.utcnow()
    res_df = res_df.set_index('newtime', drop=False)
    res_df = res_df.sort_index()
    res = cursor.execute('SELECT ID_PREDICT_DATA_HEAD, TYPE_PREDICT, TIMESTAMP FROM PREDICT_DATA_HEADERS WHERE TIMESTAMP>=:start_tme AND TIMESTAMP<:end_time AND (TYPE_PREDICT = :type_predict)',{'start_tme':start_time,'end_time':end_time,'type_predict':type_predict,'type_predict1':1}) # OR TYPE_PREDICT = :type_predict1
    list_pred_head = cursor.fetchall()
    list_pred_head = pd.DataFrame(list_pred_head, columns=['id_head', 'type_pred','newtime'])
    list_pred_head = list_pred_head.head(list_pred_head.shape[0]-1)
    #print(f'list_pred_head.shape = {list_pred_head.shape}')
    pred_df = pd.DataFrame()
    for index,pred_head in list_pred_head.iterrows():
        if index%skip_count != 0:
            pass
            continue
        head_id = pred_head[0]
        type_pred = pred_head[1]
        time_stamp = pred_head[2]
        if index%50==0:
            print(f'Line#{index} from {list_pred_head.shape[0]}. Time = {time_stamp}')
        for name, value in convert_adv_param_name.items():
            #print(f'name = {name}')
            pred = get_pred_value(cursor, head_id, value, name)
            #print(f'pred = {pred}')
            #pred = pred.copy()
            if pred.shape[0] == 0:
                continue
            #pred = pred.head(61)
            #pred = pred.tail(3)
            pred = pred.loc[start:start+length,pred.columns.values]
            #pred.sort_values('newtime', inplace=True)
            pred = pred.set_index('newtime',drop=False)
            pred = pred.sort_index()
            min_v = pred.index.min()
            max_v = pred.index.max()
            #print(name)
            if pred[name].count() == res_df.loc[min_v:max_v].count()[0]:
                res_df.loc[min_v:max_v,name] = pred[name].values
            else:
                pass
                #print(f'Не добавляю {name}, min_v = {min_v}, max_v = {max_v}')
            #print(pred)

    res_df.reset_index(drop=True, inplace=True)
    return res_df

def get_history_analityc_data(cursor, bgs_id, convert_param_name, min_tst=None, max_tst = None, drop=False):
    if max_tst is not None:
        max_time_stamp = max_tst
    else:
        res = cursor.execute('SELECT MAX("TIMESTAMP") FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "VALIDATE_STATUS">=:validate_status',{"bgs_id": bgs_id, 'validate_status':VALIDATE_LINE})
        max_time_stamp:datetime.datetime = cursor.fetchone()[0]
        if max_time_stamp is None:
            return None, None
        max_time_stamp = max_time_stamp.replace(second=0,microsecond=0)+datetime.timedelta(minutes=1)
    if min_tst is not None:
        min_time_stamp = min_tst
    else:
        res = cursor.execute(
            'SELECT MIN("TIMESTAMP") FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "VALIDATE_STATUS">=:validate_status',
            {"bgs_id": bgs_id, 'validate_status': VALIDATE_LINE})
        min_time_stamp: datetime.datetime = cursor.fetchone()[0]
        min_time_stamp = min_time_stamp.replace(second=0, microsecond=0)

    result = None
    tst = datetime.datetime.utcnow()
    res = cursor.execute(
        'SELECT DISTINCT "TIMESTAMP" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "TIMESTAMP">=:min_tst AND "TIMESTAMP"<:max_tst AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"',
        {"bgs_id": bgs_id, "min_tst": min_time_stamp, "max_tst": max_time_stamp,
         'validate_status': VALIDATE_LINE})
    res = cursor.fetchall()
    if len(res) >0:
        result = pd.DataFrame(res, columns=['newtime'])  # , dtype={param_: float}
        result.sort_values('newtime', inplace=True)
        result.set_index('newtime', drop=False, inplace=True)
    print(f'Read newtime for {datetime.datetime.utcnow()-tst}')

    for param_, param in convert_param_name.items():
        print(f'Читаю {param}')
        res = cursor.execute('SELECT "TIMESTAMP","PARAM_VALUE" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "PARAM_NAME" = :param_name AND "TIMESTAMP">=:min_tst AND "TIMESTAMP"<:max_tst AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"', {"bgs_id":bgs_id, "param_name":param, "min_tst":min_time_stamp, "max_tst":max_time_stamp, 'validate_status':VALIDATE_LINE})
        res = cursor.fetchall()
        #print(f'res.shape = {len(res)}')
        if len(res)>0:
            res_df = pd.DataFrame(res,columns=['newtime',param_]) #, dtype={param_: float}
            res_df = res_df.astype({param_: float}) #, dtype=['datetime','float']
            res_df.drop_duplicates(subset=['newtime'], keep='last',inplace=True)
            res_df.sort_values('newtime',inplace=True)
            res_df.set_index('newtime',drop=False,inplace=True)
            #print(f'res_df = {res_df}')
            if result is None:
                result = res_df
                print(f'result.shape = {result.shape}')
            else:
                result[param_] = res_df[param_]
                print(f'result.shape = {result.shape}')
    if drop:
        clmns = list(result.columns.values)
        clmns.remove('newtime')
        print(f'clmns = {clmns}')

        result.dropna(how='all', subset=clmns, inplace=True)
    max_time_stamp = result['newtime'].max()
    return result, max_time_stamp

def get_history_data(cursor, bgs_id, convert_param_name, min_tst=None, max_tst = None):
    param_name_list = list(convert_param_name.keys())
    if max_tst is not None:
        max_time_stamp = max_tst
    else:
        res = cursor.execute('SELECT MAX("TIMESTAMP") FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "VALIDATE_STATUS">=:validate_status',{"bgs_id": bgs_id, 'validate_status':VALIDATE_LINE})
        max_time_stamp:datetime.datetime = cursor.fetchone()[0]
        max_time_stamp = max_time_stamp.replace(second=0,microsecond=0)+datetime.timedelta(minutes=1)
    if min_tst is not None:
        min_time_stamp = min_tst
    else:
        res = cursor.execute(
            'SELECT MIN("TIMESTAMP") FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "VALIDATE_STATUS">=:validate_status',
            {"bgs_id": bgs_id, 'validate_status': VALIDATE_LINE})
        min_time_stamp: datetime.datetime = cursor.fetchone()[0]
        min_time_stamp = min_time_stamp.replace(second=0, microsecond=0)
    print(f'min_time_stamp = {min_time_stamp}, max_time_stamp = {max_time_stamp}')
    result = None
    ar=None
    #print(f'min_time_stamp = {min_time_stamp}')
    #print(f'convert_param_name {convert_param_name}')
    for param_, param in convert_param_name.items():
        #print(f'Читаю {param}')
        res = cursor.execute('SELECT "TIMESTAMP","PARAM_VALUE" FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "PARAM_NAME" = :param_name AND "TIMESTAMP">=:min_tst AND "TIMESTAMP"<:max_tst AND "VALIDATE_STATUS">=:validate_status ORDER BY "TIMESTAMP"', {"bgs_id":bgs_id, "param_name":param, "min_tst":min_time_stamp, "max_tst":max_time_stamp, 'validate_status':VALIDATE_LINE})
        res = cursor.fetchall()
        #print(f'res.shape = {len(res)}')
        if len(res)>0:
            res_df = pd.DataFrame(res,columns=['newtime',param_]) #, dtype={param_: float}
            res_df = res_df.astype({param_: float}) #, dtype=['datetime','float']
            #print(f'res_df.shape = {res_df.shape}')
            res_df.drop_duplicates(subset=['newtime'], keep='last',inplace=True)
            #res_df.sort_values('newtime',inplace=True)
            res_df.set_index('newtime',drop=False,inplace=True)
            #print(f'res_df = {res_df}')
            if result is None:
                result = res_df
            else:
                result[param_] = res_df[param_]
    return result, max_time_stamp



def load_json(name):
    with open(name) as f:
        dt = json.load(f)
    return dt

STATE_TYPES = {'deephistory':1,'state':2,'product':3, 'targ_retur':4}
PRODUCT_CONV = {'':'ANY','1':'CaNS','2':'NS','3':'IAS'}

def load_state(cursor, bgs_id, type_state, tst = None):
    if tst is None:
        res = cursor.execute('SELECT  MAX("TIMESTAMP_STATE") FROM "URALCHEM"."APP_STATE" WHERE "TYPE_STATE"=:type_state AND "BGS_ID"=:bgs_id',
        {'bgs_id': bgs_id,'type_state':type_state})
    else:
        res = cursor.execute(
            'SELECT  MAX("TIMESTAMP_STATE") FROM "URALCHEM"."APP_STATE" WHERE "TYPE_STATE"=:type_state AND "BGS_ID"=:bgs_id AND "TIMESTAMP_STATE"<=:max_tst',
            {'bgs_id': bgs_id, 'type_state': type_state, 'max_tst':tst})
    max_tst=cursor.fetchone()[0]
    #print(max_tst)
    res = cursor.execute('SELECT "STATE_VALUE" FROM "URALCHEM"."APP_STATE" WHERE "TYPE_STATE"=:type_state AND "BGS_ID"=:bgs_id AND "TIMESTAMP_STATE"=:max_tst',
    {'bgs_id': bgs_id,'type_state':type_state,'max_tst':max_tst})
    vl=cursor.fetchone()[0]
    #print(vl)
    return vl


def load_state_history(cursor, bgs_id, type_state, min_tst = datetime.datetime.strptime('2015-01-01 00:00','%Y-%m-%d %H:%M')):
    res = cursor.execute('SELECT "TIMESTAMP_STATE","STATE_VALUE" FROM "URALCHEM"."APP_STATE" WHERE "TYPE_STATE"=:type_state AND "BGS_ID"=:bgs_id AND "TIMESTAMP_STATE">=:min_tst',
    {'bgs_id': bgs_id,'type_state':type_state,'min_tst':min_tst})
    vl=cursor.fetchall()
    #print(vl)
    return vl


def save_model2db(conn, cursor, bgs_id, filename, properties, score_name = 'score'):
    #return None
    tst = properties['timestamp']
    score = properties[score_name]
    product = properties['product']
    deep = properties['deep']
    properties['timestamp'] = tst.strftime('%Y-%m-%d %H:%M')
    prprts = json.dumps(properties)
    with open(filename, "rb") as fl:
        model = fl.read()
    res = cursor.execute(
    'INSERT INTO "URALCHEM"."MODELS" ("TIMESTAMP","BGS_ID","SCORE","PRODUCT","DEEP","MODEL","PROPERTIES") VALUES(:tst, :bgs_id, :score, :product, :deep, :model, :properties)',
    {'tst': tst, 'bgs_id':bgs_id, 'score':score, "product":product, "deep":deep, 'model':model, 'properties':prprts})
    #conn.commit() Коммит вынесен для сохранения всех обученных моделей одного бгс за один раз, иначе будет загружаться первая обученная модель всегда...
    return res

def load_model4db(cursor, model_id, filename='modeltmp.dump'):
    res = cursor.execute(
    'SELECT "MODEL" FROM "URALCHEM"."MODELS" WHERE "ID_MODELS"=:model_id', { 'model_id':model_id})
    res = cursor.fetchone()[0]
    with open(filename,'wb') as fl:
        fl.write(res)
    model = joblib.load(filename)
    print(f'Loaded model id = {model_id}')
    return model

def find_best_model(cursor, bgs_id, tst_ld_mdl=datetime.datetime.strptime('2015-01-01 00:00','%Y-%m-%d %H:%M')):
    res = cursor.execute('SELECT MAX("TIMESTAMP") FROM "URALCHEM"."MODELS" WHERE "BGS_ID"=:bgs_id', { 'bgs_id':bgs_id})
    max_tst = cursor.fetchone()[0]
    #print(f'max_tst for load best model = {max_tst}')
    if max_tst <= tst_ld_mdl:
        return None, None, None
    res = cursor.execute('SELECT MIN("SCORE") FROM "URALCHEM"."MODELS" WHERE "BGS_ID"=:bgs_id AND "TIMESTAMP"=:max_tst', {'bgs_id':bgs_id,'max_tst':max_tst})
    min_score = cursor.fetchone()[0]
    res = cursor.execute('SELECT "ID_MODELS" FROM "URALCHEM"."MODELS" WHERE "BGS_ID"=:bgs_id AND "TIMESTAMP"=:max_tst AND "SCORE"=:min_score', {'bgs_id':bgs_id,'max_tst':max_tst,'min_score':min_score})
    model_id = cursor.fetchone()[0]
    return model_id, min_score, max_tst

def get_best_model(cursor, bgs_id, tst_ld_mdl=datetime.datetime.strptime('2015-01-01 00:00','%Y-%m-%d %H:%M')):
    model_id, score, tst = find_best_model(cursor, bgs_id, tst_ld_mdl)
    if model_id is None:
        return None, None, None, None
    model = load_model4db(cursor, model_id)
    return model, score, tst, model_id

def save_status(conn, cursor, modul, time_stamp, operation, status, code, text, bgs_id, text2=''):
    #return None
    res = cursor.execute('SELECT "URALCHEM"."ID_STATUS".NEXTVAL FROM DUMMY')
    next_id = cursor.fetchone()[0]
    res = cursor.execute(
    'INSERT INTO "URALCHEM"."STATUS" ("ID_STATUS","MODUL","TIMESTAMP","OPERATION","STATUS","ERROR_CODE","ERROR_TEXT","TEXT2","BGS_ID") VALUES(:next_id, :modul, :time_stamp, :operation, :status, :code, :text, :text2,:bgs_id)',
    {'next_id': next_id, 'modul':modul, 'time_stamp':time_stamp, 'operation':operation,"status":status, "code":code, 'text':text, 'text2':text2,'bgs_id':bgs_id})
    conn.commit()
    return res

def set_to_value_dict(trg_dict, val):
    for key, _ in trg_dict.items():
        trg_dict[key] = val
    return trg_dict

def get_last_fact_data(cursor, bgs_id, conv_params, param_name_list, count_deep, tst_last=None):
    if tst_last is None:
        tst_last = datetime.datetime.utcnow()
    res = cursor.execute('SELECT MAX("TIMESTAMP") FROM "URALCHEM"."PROCESSED_DATA" WHERE "BGS_ID" = :bgs_id AND "VALIDATE_STATUS">=:validate_status AND "TIMESTAMP"<=:tst_last',{"bgs_id": bgs_id, 'validate_status':VALIDATE_LINE, 'tst_last':tst_last})
    time_stamp:datetime.datetime = cursor.fetchone()[0]
    time_stamp = time_stamp.replace(second=0,microsecond=0) - datetime.timedelta(minutes=count_deep+1)
    result = None
    for count in range(count_deep):
        time_stamp +=datetime.timedelta(minutes=1)
        if result is None:
            result:pd.DataFrame = get_param_values(cursor, bgs_id, time_stamp, conv_params, param_name_list)
        else:
            result = result.append(get_param_values(cursor, bgs_id, time_stamp, conv_params, param_name_list), ignore_index=True)

    return result, time_stamp


def get_workshifts_times(start:datetime.datetime, end=None, hvost = False):
    if end is None:
        end = datetime.datetime.utcnow()
    step = 12
    start=start-datetime.timedelta(seconds=1)
    start = start.replace(second=0, microsecond=0)
    if start.hour < 5:
        st=start.replace(hour=5, minute=0)
    else:
        if start.hour<17:
            st=start.replace(hour=17, minute=0)
        else:
            st=start+datetime.timedelta(days=1)
            st=st.replace(hour=5, minute=0)
    nd = st+datetime.timedelta(hours=12)
    df=pd.DataFrame(columns=['start','end'])
    if hvost:
        res = {'start':start, 'end':st}
        tmp = pd.DataFrame([res])
        df = df.append(tmp, sort=False)
    while nd<end:
        res = {'start':st, 'end':nd}
        tmp = pd.DataFrame([res])
        df = df.append(tmp, sort=False)
        st = st+datetime.timedelta(hours=12)
        nd = nd + datetime.timedelta(hours=12)
    if hvost:
        res = {'start':st, 'end':end}
        tmp = pd.DataFrame([res])
        df = df.append(tmp, sort=False)
    df.reset_index(inplace=True)
    return df[['start','end']]

def get_target_retur_val(cursor, bgs_id, start, end = None):
    if end is None:
        end = datetime.datetime.utcnow()
    res = cursor.execute('SELECT "ID_STATE", "TIMESTAMP_STATE", "STATE_VALUE" FROM "URALCHEM"."APP_STATE" WHERE "TYPE_STATE"=4 AND "BGS_ID" =:bgs_id AND "TIMESTAMP_STATE"<=:end ORDER BY "ID_STATE"',{"bgs_id": bgs_id, 'start':start, 'end':end})
    res = cursor.fetchall()
    df = pd.DataFrame(res, columns=['id','start','value'])
    df['value'] = df['value'].astype(float)
    dfend = df[df['start']>start]
    dfst = df[df['start']<=start]
    df = dfst.tail(1).copy()
    df['start'] = start
    df = df.append(dfend)
    df['start'] = df['start'].map(lambda x: x.replace(second=0, microsecond=0))
    print(f'res = {df}')
    return df

def get_aligned_analityc_datetime(tst:pd.datetime):
    """
    Возвращает время в UTC сдвинутое на время сбора проб (к ближайшему меньшему из(0;4;8;12;16;20 ч. в часовом поясе МСК)
    :param tst: исходное время
    :return: сдвинутое время без минут
    """
    tst_shft = tst + pd.Timedelta(hours=3) #- datetime.timedelta(hours=1)
    hr = int(round((tst_shft.hour//4)*4,0))
    tst_shft = tst_shft.replace(hour=0,minute=0,second=0,microsecond=0)+pd.Timedelta(hours=hr)-pd.Timedelta(hours=3)

    return tst_shft

def get_areas(first, second, points_crossing):
    start = None
    areas = pd.Series()
    for index, value in points_crossing.iteritems():
        if start is None:
            start = index
            continue
        area = calculate_area(first, second, start, index)
        area = pd.Series([area], index=[index])
        areas = areas.append(area)
        start = index
    return areas


def calculate_area(first, second, start, finish):
    result = first[start:finish] - second[start:finish]
    return result.sum()


def find_crossing(first: pd.Series, second: pd.Series, val=1, rolling=20) -> pd.Series:
    """
    Вычисляет точки пересечения графиков first и second
    :param first:
    :param second:
    :param val:
    :return : серия, где 0 - пересечнеие
    +val - полож.разница между first и second
    -val - отрицательная
    """
    first = first.rolling(window=rolling, center=True, min_periods=1).mean()
    result = first.copy()
    result[:] = np.ones(len(first.values)).tolist()
    result = first - second
    result = np.clip(result, -1, 1)
    result = result.rolling(window=rolling, center=True, min_periods=1).mean()
    result2 = np.clip(result * result.shift(1), -1, 1)
    result2[result2 < 0] = 0
    result = result * result2
    result[result > 0] = val
    result[result < 0] = -val
    return result


def find_extrems_helper(ts_, shifts):
    ts = ts_.rolling(90, center=True).mean()
    extrems_ = ts.copy()
    extrems_[:] = np.ones(len(ts.values)).tolist()
    for q in shifts:
        extrems2 = ((ts >= ts.shift(q, fill_value=-1).values) &
                    (ts >= ts.shift(-q, fill_value=-1).values)
                    ).astype(int).values
        extrems_ = np.multiply(extrems_, extrems2)
    for qq in list(range(1, 15, 1)):
        q = int(qq)
        extrems_ += np.concatenate([np.array(extrems_.tolist()[q:]), np.array([0] * q)])
        extrems_ += np.concatenate([np.array([0] * q), np.array(extrems_.tolist()[:-q])])

    extrems = (extrems_).astype(bool).astype(int)
    for q in shifts:
        extrems2 = ((ts_ >= ts_.shift(q, fill_value=-1).values) &
                    (ts_ >= ts_.shift(-q, fill_value=-1).values)
                    ).astype(int).values
        extrems = np.multiply(extrems, extrems2)

    return extrems


def find_extrems(ts):
    """
    высчитывает пики по переданной pd.DataSeries
    extrems_max - возвращает маску из 0 и 1 в размер серии, где 1 - максимум
    extrems_min - возвращает маску из 0 и 1 в размер серии, где 1 - минимум
    пример использования: up,down = find_extrems(data.interpolate()['WFIR21_1\\PV'])
    """
    shifts = list(range(1, 120, 1))
    extrems_max = find_extrems_helper(ts, shifts)
    extrems_min = find_extrems_helper(-ts, shifts)

    return extrems_max, extrems_min

def get_analisys_param(df,param_name,start,curr_time,delta = 60):
    start_mean = df[(df['newtime']>=start-datetime.timedelta(minutes=delta))&(df['newtime']<start)][param_name].mean()
    curr_mean = df[(df['newtime']>=curr_time-datetime.timedelta(minutes=delta))&(df['newtime']<curr_time)][param_name].mean()
    return {'Param':param_name,'start':start,'start_val':start_mean,'curr':curr_time,'curr_val':curr_mean}

def calc_param_minmax(df, cr_df, param):
    prev_row = None
    count_mm = 0
    for row in cr_df:
        if prev_row is None:
            prev_row = row
            continue

        ar_index = df[(df['newtime'] >= prev_row) & (df['newtime'] <= row)].index
        #print(f'len(ar_index) = {len(ar_index)}, начало = {prev_row}, окончание = {row}')
        sum = (df.loc[ar_index,param+'_60']-df.loc[ar_index,param+'_720']).sum()
        df.loc[ar_index,param+'_SUM720'] = sum
        mean = (df.loc[ar_index,param+'_60']-df.loc[ar_index,param+'_720']).mean()
        df.loc[ar_index,param+'_60DELTA720'] = mean
        minutes = sum/mean
        df.loc[ar_index,param+'_DURATION720'] = minutes
        df.loc[ar_index,param+'_mm_id'] = count_mm
        df.loc[ar_index,param+'_MEAN'] = df.loc[ar_index,param].mean()
        if len(ar_index)!=0:
            if sum>0:
                mm_ind = df.loc[ar_index,param+'_60'].idxmax()
            else:
                mm_ind = df.loc[ar_index,param+'_60'].idxmin()
            df.loc[mm_ind,param+'_mm'] = df.loc[mm_ind,param+'_60']
            df.loc[ar_index,param+'_EXTREM'] = df.loc[mm_ind,param+'_60']
        prev_row = row
        count_mm+=1
    #print(f'row = {row}')
    return df

def calc_param_cross(df, param):
    df[param +'_DELTA_60DELTA720'] = df[param+'_60DELTA720'].shift(-1)-df[param+'_60DELTA720'].shift(1)
    df[param +'_DELTA_EXTREM'] = df[param+'_EXTREM'].shift(-1)-df[param+'_EXTREM'].shift(1)
    df_mm = df.dropna(subset=[param+'_mm'])
    prev_row = None
    for index, row in df_mm.iterrows():
        if prev_row is None:
            prev_row = row
            continue
        df_tmp = df[(df['newtime']>=prev_row['newtime']) & (df['newtime']<row['newtime'])]
        vl = df_tmp[df_tmp[param+'_cros720']==0][param+'_DELTA_60DELTA720'].mean()
        #print(f'val = {vl}')
        df.loc[df_tmp.index, param+'_DELTA_60DELTA720'] = vl
        vl = df_tmp[df_tmp[param+'_cros720']==0][param+'_DELTA_EXTREM'].mean()
        df.loc[df_tmp.index, param+'_DELTA_EXTREM'] = vl
        prev_row = row
    return df

def find_coherence(df, param1, param2,coef=1):
    count = 0
    df1 = df.dropna(subset = [param1+'_mm'])
    df1 = df1.append(df.tail(1))
    #print(f'df1.shape = {df1.shape}')
    df2 = df.dropna(subset = [param2+'_mm'])
    #print(f'df2.shape = {df2.shape}')
    prev_row = None
    for index, row in df1.iterrows():
        #print(f'index = {index}')
        if prev_row is None:
            prev_row = row
            prev_index = index
            continue
        #print(f'count = {count}, prev_row = {prev_row}')
        start2 = df2[(df2['newtime']>prev_row['newtime'])&((prev_row[param1+'_60DELTA720']*df2[param2+'_60DELTA720'])>0)]['newtime'].min() #
        #print(f'start2 = {start2}')
        finish2 = df2[(df2['newtime']>row['newtime'])&((row[param1+'_mm']*df2[param2+'_mm'])>0)]['newtime'].min()
        df.loc[prev_row['newtime'],param1+'_cross_id'] = count
        df.loc[start2,param2+'_cross_id'] = count
        prev_row = row
        count+=1
        df.dropna(subset=['newtime'],inplace=True)
        #df.fillna(method='ffill', inplace = True) #Нельзя заполнять всю таблицу ТУТ
    return df

def calculate_coherence(df, param1, param2):
    start = int(df[param1+'_cross_id'].min())
    finish = int(df[param2+'_cross_id'].max())
    for i in range(start, finish+1):
        df1 = df[df[param1+'_cross_id']==i]
        df2 = df[df[param2+'_cross_id']==i]
        p1_mean720 = df1[param1+'_DELTA_60DELTA720'].mean()
        p2_mean720 = df2[param2+'_DELTA_60DELTA720'].mean()
        div_mean720 = p2_mean720/p1_mean720
        df.loc[df1.index,param1+'_DIV_60DELTA720']=div_mean720
        p1_sum720 = df1[param1+'_SUM720'].mean()
        p2_sum720 = df2[param2+'_SUM720'].mean()
        div_mean720 = p2_sum720/p1_sum720
        df.loc[df1.index,param1+'_DIV_SUM720']=div_mean720

    df[param1+'_cross_id'] = df[param1+'_cross_id'].fillna(method='ffill')
    df[param2+'_cross_id'] = df[param2+'_cross_id'].fillna(method='ffill')
    #df[param1 + '_DIV_60DELTA720'] = df[param1 + '_DIV_60DELTA720'].fillna(method = 'ffill')
    #df[param1 + '_DIV_SUM720'] = df[param1 + '_DIV_SUM720'].fillna(method = 'ffill')

    return df

class AnalisysMean():
    def __init__(self, param_list, df):
        self.param_list = param_list
        self.analitic_df = df

    def get_analisys_mean(self, df, param):
        df.set_index('newtime', drop=False, inplace = True)
        df.sort_index(inplace=True)
        #print(f'Enter')
        #print(f'df.columns = {df.columns}, {df.shape}')

        df['WFIR21_720'] = df['WFIR21_raw'].rolling(720, min_periods=1).mean()
        df[param + '_720'] = df[param+'_raw'].rolling(720, min_periods=1).mean()

        df['WFIR21_60'] = df['WFIR21'].rolling(60, center=True, min_periods=1).mean() #_raw
        df[param + '_60'] = df[param].rolling(60, center=True, min_periods=1).mean() #+'_raw'

        df['WFIR21_cros720'] = find_crossing(df['WFIR21_60'], df['WFIR21_720'], rolling=60)
        df[param + '_cros720'] = find_crossing(df[param + '_60'], df[param + '_720'], rolling=60)

        df_max_time = df['newtime'].max()
        add_series = pd.Series([df_max_time], index=[df_max_time])
        cross_param = df[df[param + '_cros720'] == 0]['newtime']
        if len(cross_param)!=0:
            cross_param = cross_param.append(add_series)
        cros_w21 = df[df['WFIR21_cros720'] == 0]['newtime']
        cros_w21 = cros_w21.append(add_series)

        df = calc_param_minmax(df, cross_param, param)
        df = calc_param_minmax(df, cros_w21, 'WFIR21')

        df = calc_param_cross(df, 'WFIR21')
        df = calc_param_cross(df, param)

        df = find_coherence(df, param, 'WFIR21')
        df = calculate_coherence(df, param, 'WFIR21')

        #print(f'df.columns = {df.columns}')
        where_res = {}
        where_res[param] = where_param(df,param)
        where_res['WFIR21'] = where_param(df,'WFIR21')
        print(f'where_res = {where_res}')

        coef = find_coef_WFIR_param(df, param, where_res)
        #where_res[param]['coef'] = coef
        where_res[param]['cur_coef'] = coef['cur_coef']
        where_res[param]['next_coef'] = coef['next_coef']
        where_res['WFIR21']['cur_coef'] = 1
        where_res['WFIR21']['next_coef'] = 1
        #df.to_csv('tmptmp.csv')
        #exit(777)
        return df, where_res

#TODO Требуется доработать функцию find_last_coef
def find_coef_WFIR_param(df, param, where_res):
    deltas = {1:{'cur':1,'next':1},2:{'cur':1,'next':-1},3:{'cur':-1,'next':-1},4:{'cur':-1,'next':1}}
    whr = where_res[param]
    cross_ids = df[param+'_cross_id'].unique()
    cross_ids = cross_ids[~np.isnan(cross_ids)] #delete nan
    cross_ids = cross_ids.astype(int)
    #print(f'cross_ids = {cross_ids}')
    cur = deltas[whr['where']]['cur']
    next = deltas[whr['where']]['next']
    name_col = param+'_DELTA_60DELTA720'
    df1 = df.dropna(subset = [param+'_DELTA_60DELTA720'])
    df1 = df.dropna(subset = [param+'_DIV_60DELTA720'])
    df1 = df1[df1[name_col]!=0]
    df1 = df1[df1[param + '_DIV_60DELTA720']!=0]
    df_cur = df1[df1[name_col]*cur>0]
    df_next = df1[df1[name_col]*next>0]
    curr_cross_id = df_cur[param+"_cross_id"].unique().astype(int)
    next_cross_id = df_next[param+"_cross_id"].unique().astype(int)
    #print(f'df_cur = {curr_cross_id}, df_next = {next_cross_id}')
    cur_coef = find_norm_coef(df, param, curr_cross_id)
    next_coef = find_norm_coef(df, param, next_cross_id)
    #print(f'cur_coef = {cur_coef}, next_coef = {next_coef}')
    return {'cur_coef':cur_coef,'next_coef':next_coef}

def find_norm_coef(df, param, cross_id):
    ln = len(cross_id)
    if ln==0:
        return None
    #print(f'ln = {ln}')
    cross_id = cross_id[-1]
    #print(f'cross_id = {cross_id}')
    res = df[df[param+'_cross_id']==cross_id][param+'_DIV_60DELTA720'].mean()
    return res


def where_param(df, param):
    result = {}
    last_row = df.tail(1)
    #print(f'last_row = {last_row.columns}')
    result['newtime'] = last_row['newtime'].values[0]
    result['param'] = param
    result['target_retur'] = last_row['target_retur'].values[0]
    result['cur_delta720'] = (last_row[param+'_60'] - last_row[param+'_720']).values[0]
    result['mean720'] = last_row[param+'_720'].values[0]
    result['mean60'] = last_row[param+'_60'].values[0]
    result['60delta720'] = last_row[param+'_60DELTA720'].values[0]
    result['curr_val'] = last_row[param+'_raw'].values[0]
    result['cross_id'] = int(last_row[param+'_cross_id'].values[0])
    cros_time = df[df[param+'_cros720'] == 0]['newtime'].max()
    result['delta_time_cross'] = (last_row['newtime'][0] - cros_time)
    df_mm = df.dropna(subset=[param+'_mm'])
    mm_time = df_mm['newtime'].max()
    result['delta_time_extrem'] = (last_row['newtime'][0] - mm_time)
    #print(f"type(result['delta_time_extrem']) = {result['delta_time_extrem'][0]}")
    if result['delta_time_extrem'] < datetime.timedelta(minutes=30):
        df_mm = df_mm.drop(index=df_mm.index.max())
        mm_time = df_mm['newtime'].max()
        result['delta_time_extrem'] = (last_row['newtime'][0] - mm_time)
    result['val_extrem'] = df_mm.tail(1)[param+'_EXTREM'][0]
    #print(f"result['delta_time_extrem'] = {result['delta_time_extrem']}")
    #print(f"result['delta_time_cross'] = {result['delta_time_cross']}")
    if (result['delta_time_extrem'] < result['delta_time_cross']): # and (result['delta_time_cross']>=datetime.timedelta(minutes=20))
        if result['60delta720'] < 0: #cur_delta720
            result['where'] = 1
        else:
            result['where'] = 3
    else:
        if result['60delta720'] < 0: #cur_delta720
            result['where'] = 4
        else:
            result['where'] = 2
    return result


def get_drive_mean(df, param, val_param, analisys_dct, props, w_speed_step, w_accel_step, mean_speed_2step, mean_accel_2step, drive_step = 5):
    dct = analisys_dct['WFIR21']
    whr = dct['where']
    try:
        if whr == 1:
            val_param = get_drive_mean1(df, param, val_param, analisys_dct, props, w_speed_step, w_accel_step, mean_speed_2step, mean_accel_2step, drive_step)
        if whr == 2:
            val_param = get_drive_mean2(df, param, val_param, analisys_dct, props, w_speed_step, w_accel_step, mean_speed_2step, mean_accel_2step, drive_step)
        if whr == 3:
            val_param = get_drive_mean3(df, param, val_param, analisys_dct, props, w_speed_step, w_accel_step, mean_speed_2step, mean_accel_2step, drive_step)
        if whr == 4:
            val_param = get_drive_mean4(df, param, val_param, analisys_dct, props, w_speed_step, w_accel_step, mean_speed_2step, mean_accel_2step, drive_step)
    except:
        df.to_csv('get_drive_mean.csv')
        print(f'analisys = {analisys_dct}')
        raise
        #exit(987)
    return val_param

def get_drive_mean1(df, param, val_param, analisys_dict, props, w_speed_step30, w_accel_step30, w_speed_step60, w_accel_step60, drive_step):
    print(f'DRIVE1')
    w_analisys = analisys_dict['WFIR21']
    p_analisys = analisys_dict[param]
    #coef = props['drive_param']['coef']
    cur_coef = p_analisys['cur_coef']
    w_delta_target = w_analisys['mean60'] - w_analisys['target_retur']
    #c_delta = math.copysign(0.33, w_delta_target) #TODO переделать
    #p_delta_target = w_delta_target/cur_coef
    #print(f'p_delta_target = {p_delta_target}')
    #w_delta = w_delta_target + (w_speed_step60+w_accel_step60)
    #if w_delta>=0:
    #    return val_param
    w_speed = w_accel_step60#(w_speed_step30+w_accel_step30)*2 #w_speed = mean_speed_step60 #
    #if (w_delta*w_speed)>0: #TODO переделать
    #    return val_param
    w_time1 = abs(w_delta_target/w_speed)
    w_time = get_time_sva(abs(w_delta_target), w_speed_step30*2, abs(w_accel_step60))
    print(f'w_time_old={w_time1},w_time={w_time}')
    if w_time<(drive_step*4/60):
        w_time=drive_step*4/60
    print(f'w_time = {w_time}, w_delta_target = {w_delta_target}, w_speed_step60 = {w_speed_step60}, w_accel_step60 = {w_accel_step60}')
    param_mm_id = int(df.tail(1)[param+'_mm_id'].values[0])
    prev_60delta720 = df[df[param+'_mm_id']==(param_mm_id-1)][param+'_60DELTA720'].mean()
    print(f'prev_60delta720 = {prev_60delta720}')
    prev_60sum720 = df[df[param+'_mm_id']==(param_mm_id-1)][param+'_SUM720'].mean()
    print(f'prev_60SUM720 = {prev_60sum720}')
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    if p_analisys['where'] not in (3,4):
        return val_param
    if p_analisys['where'] == 3: #and (p_analisys['mean60'] > p_analisys['mean720'])
    #if p_analisys['mean60'] > p_analisys['mean720']:
        print(f"p_analisys['where']={p_analisys['where']}, delta60_720={p_analisys['mean60'] - p_analisys['mean720']}")
        p_delta = val_param - p_analisys['mean720'] + p_analisys['60delta720']*1.57  # + p_delta_target/3 #ТОDO коэф.
        drive = -p_delta / (w_time * 60) * drive_step * np.clip(w_time, 1, 3) * np.clip(abs(w_accel_step60),1,3)
        drive = np.clip(abs(drive), 0, props['max_step']) * math.copysign(1,drive)
    if p_analisys['where'] == 4:
        print(f"p_analisys['where']={p_analisys['where']}, delta60_720={p_analisys['mean60'] - p_analisys['mean720']}")
        p_delta = val_param - p_analisys['mean720'] + prev_60delta720 *1.57#TODO Удалить 3  # + p_delta_target/3 #ТОDO коэф.
        drive = -p_delta/(w_time * 60) * drive_step * np.clip(w_time, 1, 3) * np.clip(abs(w_accel_step60),1,3)
        drive = np.clip(abs(drive),0,props['max_step'])*math.copysign(1,drive)
        print(f'PIRC WHERE=4')
        #raise
    val_param = val_param + drive #p_analisys['mean60']
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    return val_param


def get_drive_mean2(df, param, val_param, analisys_dict, props, w_speed_step30, w_accel_step30, w_speed_step60, w_accel_step60, drive_step):
    print(f'DRIVE2')
    w_analisys = analisys_dict['WFIR21']
    p_analisys = analisys_dict[param]
    coef = props['drive_param']['coef']
    cur_coef = p_analisys['cur_coef']
    w_delta_target = w_analisys['mean60'] - w_analisys['target_retur']
    #p_delta_target = w_delta_target/cur_cross_coef
    #print(f'p_delta_target = {p_delta_target}')
    w_delta = w_delta_target + w_speed_step60
    param_mm_id = int(df.tail(1)[param+'_mm_id'].values[0])
    prev_60delta720 = df[df[param+'_mm_id']==(param_mm_id-1)][param+'_60DELTA720'].mean()
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    if w_accel_step60>=0:
        print(f'PIRC ACCEL >= 0')
        p_delta = val_param - p_analisys['mean720'] + prev_60delta720*1.57 + w_accel_step60/cur_coef #TODO Удалить * 3 # + p_delta_target/3 #ТОDO коэф.
        w_time = 0 #   abs(w_speed_step60 / w_accel_step60)-1
        w_time = np.clip(w_time, drive_step*4/60, 3)
        drive = -p_delta / w_time / 60 * drive_step
    else:
        print(f'PIRC ACCEL < 0')
        p_delta = val_param - p_analisys['mean720'] #+ prev_60delta720*3  #TODO Переделать на связку с w_accel_step60
        w_time1 = abs(w_delta/w_accel_step60)
        w_time = abs(w_speed_step60 / w_accel_step60)
        w_time = w_time+get_time_sva(abs(w_delta_target), abs(w_speed_step30) * 2, abs(w_accel_step60))
        print(f'2.2.w_time_old={w_time1}, w_time={w_time} = w_delta_target={w_delta_target}, w_speed_step30={w_speed_step30},w_accel_step60={w_accel_step60}')
        #w_time = np.clip(w_time, drive_step/60, 3)
        if w_time<6:
            print(f'TIME 2 TARGET < 6')
            drive = -p_delta/w_time1/60*drive_step
        else:
            print(f'TIME 2 TARGET >= 6')
            drive = -p_delta/w_time/60*drive_step



    drive = np.clip(abs(drive), 0, props['max_step']) * math.copysign(1, drive)
    val_param = val_param + drive #p_analisys['mean60']
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    return val_param


def get_drive_mean3(df, param, val_param, analisys_dict, props, w_speed_step30, w_accel_step30, w_speed_step60, w_accel_step60, drive_step):
    print(f'DRIVE3')
    w_analisys = analisys_dict['WFIR21']
    p_analisys = analisys_dict[param]
    coef = props['drive_param']['coef']
    cur_coef = p_analisys['cur_coef']
    w_delta_target = w_analisys['mean60'] - w_analisys['target_retur']
    #c_delta = math.copysign(0.33, w_delta_target)
    #p_delta_target = w_delta_target/cur_coef
    #print(f'p_delta_target = {p_delta_target}')
    w_delta = w_delta_target + w_speed_step60 # w_analisys['mean60'] - w_analisys['mean720']
    #if w_delta>=0:
    #    return val_param
    w_speed = w_accel_step60 #w_speed = mean_speed_step60 #
    #if (w_delta*w_speed)>0: #TODO доделать
    #    return val_param
    w_time1 = abs(w_delta/w_speed)
    w_time = get_time_sva(abs(w_delta_target), abs(w_speed_step30) * 2, abs(w_accel_step60))
    print(
        f'3.w_time_old={w_time1}, w_time={w_time} = w_delta_target={w_delta_target}, w_speed_step30={w_speed_step30},w_accel_step60={w_accel_step60}')
    if w_time<(drive_step*4/60):
        w_time=drive_step*4/60
    print(f'w_time = {w_time}')
    param_mm_id = int(df.tail(1)[param+'_mm_id'].values[0])
    prev_60delta720 = df[df[param+'_mm_id']==(param_mm_id-1)][param+'_60DELTA720'].mean()

    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}

    if p_analisys['where'] in (3,4):
        return val_param

    if p_analisys['where'] == 1:
        p_delta = val_param - p_analisys['mean720'] + p_analisys['60delta720']*1.57  # + p_delta_target/3 #ТОDO коэф.
        drive = -p_delta / (w_time * 60) * drive_step * np.clip(w_time, 1, 3) * np.clip(abs(w_accel_step60),1,3)
        drive = np.clip(abs(drive), 0, props['max_step']) * math.copysign(1, drive)
        print(f'PIRC WHERE = 1')

    if p_analisys['where'] == 2:
        p_delta = val_param - p_analisys['mean720'] + prev_60delta720*1.57 # + p_delta_target/3 #ТОDO коэф.
        drive = -p_delta / (w_time * 60) * drive_step * np.clip(w_time, 1, 3) * np.clip(abs(w_accel_step60),1,3)
        drive = np.clip(abs(drive),0,props['max_step'])*math.copysign(1,drive)
        print(f'PIRC WHERE=2')
    val_param = val_param + drive #p_analisys['mean60']
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    return val_param



def get_drive_mean4(df, param, val_param, analisys_dict, props, w_speed_step30, w_accel_step30, w_speed_step60, w_accel_step60, drive_step):
    print(f'DRIVE4')
    w_analisys = analisys_dict['WFIR21']
    p_analisys = analisys_dict[param]
    coef = props['drive_param']['coef']
    cur_coef = p_analisys['cur_coef']
    w_delta_target = w_analisys['mean720'] - w_analisys['target_retur']
    #p_delta_target = w_delta_target/cur_cross_coef
    #print(f'p_delta_target = {p_delta_target}')
    #w_delta = w_analisys['mean60'] - w_analisys['mean720']
    #w_speed = (w_speed_step30+w_accel_step30)*2
    #w_time = abs(w_delta/w_speed)
    param_mm_id = int(df.tail(1)[param+'_mm_id'].values[0])
    prev_60delta720 = df[df[param+'_mm_id']==(param_mm_id-1)][param+'_60DELTA720'].mean()
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    if w_accel_step60<=0:
        print(f'PIRC ACCEL <= 0')
        p_delta = val_param - p_analisys['mean720'] + prev_60delta720*1.57 + w_accel_step60/cur_coef #TODO +w_accel_step60
        w_time = drive_step*4/60 #abs(w_speed_step60 / w_accel_step60) # TODO - dhtvz ytrjhhtrnyj cxbnftncz
        w_time = np.clip(w_time, drive_step*4/60, 3)
        drive = -p_delta / w_time / 60 * drive_step
        print(f'4.1.w_time={w_time} = w_delta_target={w_delta_target}, w_speed_step30={w_speed_step30},w_accel_step60={w_accel_step60}')
    else:
        print(f'PIRC ACCEL > 0')
        p_delta = val_param - p_analisys['mean720'] # + p_delta_target/3 #ТОDO коэф.
        w_time1 = abs(w_delta_target/w_accel_step60)
        w_time_2_mm = abs(w_speed_step60 / w_accel_step60)
        w_time = w_time_2_mm+get_time_sva(abs(w_delta_target), abs(w_speed_step30) * 2, abs(w_accel_step60))
        print(f'4.2.w_time_old={w_time1}, w_time={w_time} = w_delta_target={w_delta_target}, w_speed_step30={w_speed_step30},w_accel_step60={w_accel_step60}')
        #w_time = np.clip(w_time, drive_step/60, 3)
        if w_time<6:
            print(f'TIME 2 TARGET < 6')
            drive = -p_delta/ w_time_2_mm/60*drive_step
        else:
            print(f'TIME 2 TARGET >= 6')
            drive = -p_delta/w_time/60*drive_step


    #w_time1 = abs(w_speed_step60 / w_accel_step60)
    #w_time = get_time_sva(abs(w_delta_target), abs(w_speed_step30) * 2, abs(w_accel_step60))
    #print(f'3.w_time_old={w_time1}, w_time={w_time} = w_delta_target={w_delta_target}, w_speed_step30={w_speed_step30},w_accel_step60={w_accel_step60}')
    #w_time = np.clip(w_time, 5 / 60, 3)
    #drive = -p_delta/w_time/60*5
    drive = np.clip(abs(drive), 0, props['max_step']) * math.copysign(1, drive)
    val_param = val_param + drive #p_analisys['mean60']
    print(f"val_param={val_param},p_analisys['mean720']={p_analisys['mean720']},60delta720={p_analisys['60delta720']},prev_60delta720={prev_60delta720}") #delta_p={delta_p}
    return val_param


def get_time_sva(s, v, a):
    tmp = 2*(s-v)/a
    if tmp<0:
        print(f'!!!GET TIME MINUS---')
        tmp=s/(v+a/2)
    else:
        tmp = tmp**0.5
    return tmp

def linear_interpolation(arr : np.array) -> float:
    lr = LinearRegression().fit(np.array(range(len(arr))).reshape(-1,1), arr)
    x = lr.predict(np.array(range(len(arr))).reshape(-1,1)).ravel()[-1]
    return x

def predict_next_points(x: np.array, rolling_apply: int = 30, horizon: int = 60) -> np.array:
    if x.shape[0] < rolling_apply + horizon:
        raise ValueError("length of x is too small, make it at least " + str(rolling_apply+horizon) + " points")
    df = pd.DataFrame({'x' : x})
    df['x'] = df['x'].rolling(rolling_apply).apply(linear_interpolation, raw=True).fillna(0)
    x = df.x.values[rolling_apply:]
    velocities = []
    for i in range(horizon):
        velocities.append((x[(i+1):] - x[:-(i+1)])[-1] / (i+1))
    velocities = np.array(velocities)
    accs = []
    for i in range(horizon-2):
        accs.append((velocities[-1] - velocities[-i-2]) / (i+1))
    accs = np.array(accs)
    res = []
    for i in range(horizon):
        pnt = x[-1]
        for j in range(1, horizon, 1):
            pnt += velocities[j-1] * (i * 1.0) / len(list(range(1, horizon, 1)))
        for j in range(1, horizon-2, 1):
            pnt += accs[j-1] * (i**2.0) / 2.0 / len(list(range(1, horizon, 1)))
        res.append(pnt)
    return np.array(res)

def predict_velocities_accs(x: np.array, rolling_apply: int = 30, horizon: int = 60) -> np.array:
    from scipy.signal import savgol_filter
    if x.shape[0] < rolling_apply + horizon:
        raise ValueError("length of x is too small, make it at least " + str(rolling_apply+horizon) + " points")
    #x = savgol_filter(x, 201, 3)
    velocities = []
    for i in range(horizon):
        # v * t
        velocities.append((x[(i+1):] - x[:-(i+1)])[-1] / (i+1))
    velocities = np.array(velocities)
    accs = []
    for i in range(horizon-2):
        # a * t^2 / 2.0
        accs.append((velocities[-1] - velocities[-i-2]) / ((i+1) ** 2) ** 0.5 )
    accs = np.array(accs)
    return np.mean(velocities), np.mean(accs)

def predict_next_point_from_df(df:pd.DataFrame, name:str, polling_apply: int = 30, horizon: int = 60, smooth=20):
    df = df.tail((polling_apply+horizon)*2)
    df_cp = df.copy()
    df_cp[name+'_mn']=df[name].rolling(window=smooth,min_periods=1,center=True).mean() #
    x=df_cp[name+'_mn'].values
    y=predict_next_points(x,polling_apply,horizon)
    y_len = y.shape[0]
    y = pd.DataFrame(y,columns=[name])
    y[name]=y[name].rolling(window=smooth,min_periods=1).mean() #,center=True
    #print(f"y={y}")
    #print(f"df['newtime']={df['newtime']}")
    df = df.tail(y_len)
    df.reset_index(inplace=True)
    y['newtime']=df['newtime']
    y['newtime'] = y['newtime']+pd.Timedelta(minutes=horizon)
    y.set_index('newtime', drop=False, inplace=True)
    return y

    params_limit = {
        'PIRC4463': {
            'diff': 0.3,
            'time_diff': 15
        },
        'TIR812': {
            'diff': 2,
            'time_diff': 70
        },
        'PIR16': {
            'diff': 1,
            'time_diff': 20
        },
    }

def estimate_correlation(df:pd.DataFrame, name_param, name_adv, props, averaging_time, level = 1):
    # get parameters
    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    td = props['time_diff']
    abs_limit = props['diff']
    # get original series and averaging
    #series_orig = df[['newtime', name_param]].copy()
    #series_orig.index = series_orig.newtime
    df[name_param+'mn'] = df[name_param].rolling(averaging_time).mean()
    #series_test = df[['newtime', name_adv]].copy()
    #series_test.index = series_test.newtime
    # check lag on td-time
    perc = 0.
    if name_adv in df.columns:
        df['is_ok_lag'] = (df[name_adv] - df[name_param+'mn'].shift(-td)).abs() <= abs_limit
        # check 9-min lag (min 10 frequency of adviser)
        df['is_ok_cur'] = (df[name_adv] - df[name_param+'mn'].shift(-9)).abs() <= abs_limit
        # null should be treated as a correct values
        df['is_ok'] = df['is_ok_lag'] | df['is_ok_cur']
        df.loc[df[df['is_ok']].index,'YES_ADV_'+name_param] = df.loc[df[df['is_ok']].index,name_adv]
        df = df[df['FIRC841'] > level]
        count1=df[df['is_ok']].shape[0]
        count0=df[df['is_ok']!=True].shape[0]
        if count1+count0==0:
            perc = 0
        else:
            perc = round(float(count1)/(count0+count1)*100,2)
    df.set_index('newtime', drop=False, inplace=True)
    return df, perc

def get_abs_water_from_humidity(temp, hum):
    mx_wt = get_mx_water_from_temp(temp)
    return hum/100. * mx_wt

DF_WT_TEMP = None
def get_mx_water_from_temp(temp):
    global DF_WT_TEMP
    if DF_WT_TEMP is None:
        x=((-30,0.29),(-20,0.81),(-10,2.1),(0,4.8),(10,9.4),(20,17.3),(30,30.4),(40,51.1),(50,83),(60,130),(70,198),(80,293),(90,423),(100,598))
        t = [ i for i in range(-30,101,1)]
        df = pd.DataFrame(t, columns=['temp'])
        df.set_index('temp',drop=False,inplace=True)
        tbl_df = pd.DataFrame(x,columns=['temp','wt'])
        #print(f'tbl_df = {tbl_df}')
        tbl_df.set_index('temp',drop=False,inplace=True)
        df['wt'] = tbl_df['wt']
        df['wt'] = df['wt'].interpolate(method='polynomial',order = 5)
        DF_WT_TEMP = df
        #df['delta'] = df['wt'] - tbl_df['wt']
    else:
        df = DF_WT_TEMP
    #print(f'df = {df}')
    df_cp = df.copy()
    #df_cp.dropna(inplace=True)
    #print(f'df_cp = {df_cp}')
    temp = np.clip(temp,df['temp'].min(),df['temp'].max())
    return df.loc[int(temp),'wt']

def get_analisysfromdf(res_df,bgs_id,desc,start,finish,firc841_level_val):
    loc_df = res_df[(res_df['newtime'] >= start) & (res_df['newtime'] < finish) & (res_df['FIRC841'] > firc841_level_val)].copy()
    if loc_df.shape[0]>0:
        #print(f'loc_df.shape = {loc_df.shape} start = {start} finish = {finish}')
        #print(f'res_df.shape = {res_df.shape}, loc_df.shape = {loc_df.shape}')
        loc_df.loc[:,'good_2_5'] = loc_df.loc[:,"FIRC841"]*loc_df.loc[:,'KS_2_5_align']/100
        loc_df.loc[:,'bad_2_5'] = loc_df.loc[:,"FIRC841"]*(100.-loc_df['KS_2_5_align'])/100

        loc_df.loc[:,'good_2'] = loc_df.loc[:,"FIRC841"]*(100.-loc_df.loc[:,'KS_2_align'])/100
        loc_df.loc[:,'bad_2'] = loc_df.loc[:,"FIRC841"]*(loc_df['KS_2_align'])/100

        df_good_Dcp = loc_df[(loc_df['KS_Dcp']>=3.1) & (loc_df['KS_Dcp']<=3.5)].copy()
        loc_df.loc[:,'good_Dcp'] = 0.
        loc_df.loc[df_good_Dcp.index,"good_Dcp"] = 1.
        loc_df.loc[:,'good_Dcp'] = loc_df.loc[:,'good_Dcp'] * loc_df.loc[:,'FIRC841']
        loc_df.loc[:,'bad_Dcp'] = 1.
        loc_df.loc[df_good_Dcp.index,"bad_Dcp"] = 0.
        loc_df.loc[:,'bad_Dcp'] = loc_df.loc[:,'bad_Dcp'] * loc_df.loc[:,'FIRC841']



        loc_dict = {'BGS': bgs_id, 'name': desc, 'start': start, 'finish': finish, 'period': finish - start
            , 'WFIR21': loc_df["WFIR21"].mean(), 'WFIR21_STD': loc_df["WFIR21"].std(), 'FIRC841': loc_df["FIRC841"].mean()
            , 'PIRC4463': loc_df["PIRC4463"].mean(), 'TIR811': loc_df["TIR811"].mean(), 'TIR812': loc_df["TIR812"].mean()
            , 'FIRC17': loc_df["FIRC17"].mean(), 'PIR16': loc_df["PIR16"].mean(), 'KS_2_5': loc_df["KS_2_5"].mean()
            , 'KS_Dcp': loc_df["KS_Dcp"].mean(), 'KS_Dcp_STD': loc_df["KS_Dcp"].std(), 'KS_2': loc_df["KS_2"].mean()
            , 'KS_2_align': loc_df["KS_2_align"].mean(), 'KS>5': loc_df["KS_5_6"].mean() + loc_df["KS_6_3"].mean()
            , 'Выпущено':loc_df["FIRC841"].sum()*1.25/60,'Вып.станд.2_5':loc_df["good_2_5"].sum()*1.25/60
            , 'Вып.отсева.2_5':loc_df["bad_2_5"].sum()*1.25/60,'Вып.станд.Dcp':loc_df["good_Dcp"].sum()*1.25/60
            , 'Вып.отсева.Dcp':loc_df["bad_Dcp"].sum()*1.25/60,'Вып.станд.2':loc_df["good_2"].sum()*1.25/60
            , 'Вып.отсева.2':loc_df["bad_2"].sum()*1.25/60,}
        return loc_dict
    return {}

def convert_hist_data(filename):
    df = pd.read_csv(filename)  #'.//histdata//analog_df_.csv' './/histdata//test.csv'
    print(f'df.columns = {df.columns}')
    # df = df.tail(10)
    # df.to_csv('.//histdata//test.csv', index=False)
    dfs = {1: pd.DataFrame(), 2: pd.DataFrame(), 3: pd.DataFrame()}
    dfs[1]['newtime'] = df['newtime']
    dfs[2]['newtime'] = df['newtime']
    dfs[3]['newtime'] = df['newtime']
    for column in df.columns:
        # print(f'column = {column}')
        n = column.find('\\')
        if n != -1:
            name1 = column[n + 1:-1]
            n_end = column.find('_')
            name = column[n + 1:n_end]
            bgs_id = int(column[n_end + 1:n_end + 2])
            print(f'column = {column} bgs_id = {bgs_id} name = {name}')
            if bgs_id in dfs.keys():
                dfs[bgs_id][name] = df[column]
    #dfs[1].to_csv('.//histdata//histdata1.csv', index=False)
    #dfs[2].to_csv('.//histdata//histdata2.csv', index=False)
    #dfs[3].to_csv('.//histdata//histdata3.csv', index=False)
    return dfs

def convert_hist_data(product):
    product = product
    df = pd.read_csv(f'.//histdata//{product}.csv') #'.//histdata//test.csv'
    print(f'df.columns = {df.columns}')
    dfs = {"0":pd.DataFrame(),"1":pd.DataFrame(),"2":pd.DataFrame(),"3":pd.DataFrame()}
    convert = {"0":"0","6":"3","5":"2","4":"1",'7':'0'}
    product_dict = {"CaNS":1, "NS":2, "IAS":3}
    for key in dfs:
        dfs[key]['newtime'] = df['datetime']
    for column in df.columns:
        n = column.find("-")
        bgs_str = '0'
        if n!=-1:
            bgs_str = column[n+1:n+2]
        name = column.replace("БГС","BGS")
        name = name.replace("КС","KS")
        name = name.replace("Хим.состав ","")
        name = name.replace('BGS-4',"BGS")
        name = name.replace('BGS-5',"BGS")
        name = name.replace('BGS-6',"BGS")
        name = name.replace('KS-4',"KS")
        name = name.replace('KS-5',"KS")
        name = name.replace('KS-6',"KS")
        name = name.replace('Д 61-5',"D61_5")
        name = name.replace('Д 61-6',"D61_6")
        name = name.replace('Д 61-7',"D61_7")
        name = name.replace('Д 61',"D61")
        name = name.replace('<',"")
        name = name.replace('>',"")
        name = name.replace('(',"")
        name = name.replace(')',"")
        name = name.replace(',15',"")
        name = name.replace('-',"_")
        name = name.replace(',',"_")
        name = name.replace('5_6_3',"5_6")
        name = name.replace('Dср','Dcp')
        name = name.replace('datetime',"newtime")
        print(f'bgs = {bgs_str} column = {column} --> {name}')
        if convert[bgs_str] in dfs.keys():
            dfs[convert[bgs_str]][name] = df[column]
    for key in dfs.keys():
        setcol = dfs[key].columns.to_list()
        setcol.remove('newtime')
        print(f'setcol = {setcol}')
        dfs[key] = dfs[key].dropna(how='all',subset=setcol)
        dfs[key]['product'] = product_dict[product]

    print(f'dfs[1] = {dfs["1"].tail(5)}')
    for key in dfs:
        pass
        #dfs[key].to_csv((f'.//histdata//{product}{key}.csv'), index=False)

    return dfs

def add_analitic2hist():
    for bgs_str in ('1', '2', '3'):
        df = pd.read_csv(f'.//histdata//histdata{bgs_str}.csv')
        df.dropna(how='any', subset=['newtime', ], inplace=True)
        df.drop_duplicates(subset=['newtime', ], inplace=True)
        df['newtime'] = pd.to_datetime(df['newtime'])
        df.sort_values('newtime', inplace=True)
        df.set_index('newtime', drop=False, inplace=True)
        for product in ('CaNS', 'NS', 'IAS'):
            andf = pd.read_csv(f'.//histdata//{product}{bgs_str}.csv')  # './/histdata//test.csv'
            andf.dropna(how='any', subset=['newtime', ], inplace=True)
            andf.drop_duplicates(subset=['newtime', ], inplace=True)
            andf['newtime'] = pd.to_datetime(andf['newtime'])
            andf.sort_values('newtime', inplace=True)
            andf.set_index('newtime', drop=False, inplace=True)
            setcol = andf.columns.to_list()
            setcol.remove('newtime')
            ind = df.index & andf.index
            for column in setcol:
                print(f'bgs {bgs_str} product {product} column = {column}')
                df.loc[ind, column] = andf.loc[ind, column]
        print(f'Записываю bgs{bgs_str}')
        df['newtime'] = df['newtime'] - pd.Timedelta(hours=3)
        #df.to_csv(f'.//histdata//HistDatAll{bgs_str}.csv', index=False)
        return None

def convert_hist_analityc_data():

    fl_dict = {"КаНС": "CaNS", "НС": "NS", "ИАС": "IAS"}
    for nm1, nm2 in fl_dict.items():
        df = pd.read_csv(f'.//histdata//{nm1}.csv', delimiter=';', encoding='cp1251')
        df['newtime'] = df['date'] + " " + df['time']
        df.drop(columns=['date', 'time'], inplace=True)
        print(f'df.columns = {df.columns}')
        df.to_csv(f'.//histdata//{nm2}.csv', index=False)

    products = ('CaNS', 'NS', 'IAS',)  # ,
    for product in products:
        df = pd.read_csv(f'.//histdata//{product}.csv')  # './/histdata//test.csv'
        print(f'df.columns = {df.columns}')
        dfs = {"0": pd.DataFrame(), "1": pd.DataFrame(), "2": pd.DataFrame(), "3": pd.DataFrame()}
        convert = {"0": "0", "6": "3", "5": "2", "4": "1", '7': '0'}
        product_dict = {"CaNS": 1, "NS": 2, "IAS": 3}
        for key in dfs:
            dfs[key]['newtime'] = df['newtime']
        for column in df.columns:
            n = column.find("-")
            bgs_str = '0'
            if n != -1:
                bgs_str = column[n + 1:n + 2]
            name = column.replace("БГС", "BGS")
            name = name.replace("КС", "KS")
            name = name.replace("Хим.состав ", "")
            name = name.replace('BGS-4', "BGS")
            name = name.replace('BGS-5', "BGS")
            name = name.replace('BGS-6', "BGS")
            name = name.replace('KS-4', "KS")
            name = name.replace('KS-5', "KS")
            name = name.replace('KS-6', "KS")
            name = name.replace('Д 61-5', "D61_5")
            name = name.replace('Д 61-6', "D61_6")
            name = name.replace('Д 61-7', "D61_7")
            name = name.replace('Д 61', "D61")
            name = name.replace('<', "")
            name = name.replace('>', "")
            name = name.replace('(', "")
            name = name.replace(')', "")
            name = name.replace(',15', "")
            name = name.replace('-', "_")
            name = name.replace(',', "_")
            name = name.replace('5_6_3', "5_6")
            name = name.replace('Dср', 'Dcp')
            name = name.replace('datetime', "newtime")
            print(f'bgs = {bgs_str} column = {column} --> {name}')
            if convert[bgs_str] in dfs.keys():
                dfs[convert[bgs_str]][name] = df[column]
        for key in dfs.keys():
            setcol = dfs[key].columns.to_list()
            setcol.remove('newtime')
            print(f'setcol = {setcol}')
            dfs[key] = dfs[key].dropna(how='all', subset=setcol)
            dfs[key]['product'] = product_dict[product]

        print(f'dfs[1] = {dfs["1"].tail(5)}')
        for key in dfs:
            dfs[key].to_csv((f'.//histdata//{product}{key}.csv'), index=False)

    for bgs_str in ('1', '2', '3'):
        df = pd.read_csv(f'.//histdata//histdata{bgs_str}.csv')
        df.dropna(how='any', subset=['newtime', ], inplace=True)
        df.drop_duplicates(subset=['newtime', ], inplace=True)
        df['newtime'] = pd.to_datetime(df['newtime'], format='%Y-%m-%d %H:%M')
        df.sort_values('newtime', inplace=True)
        df.set_index('newtime', drop=False, inplace=True)
        for product in ('CaNS', 'NS', 'IAS'):
            andf = pd.read_csv(f'.//histdata//{product}{bgs_str}.csv')  # './/histdata//test.csv'
            andf.dropna(how='any', subset=['newtime', ], inplace=True)
            andf.drop_duplicates(subset=['newtime', ], inplace=True)
            andf['newtime'] = pd.to_datetime(andf['newtime'], format='%d.%m.%Y %H:%M')
            andf.sort_values('newtime', inplace=True)
            andf.set_index('newtime', drop=False, inplace=True)
            setcol = andf.columns.to_list()
            setcol.remove('newtime')
            ind = df.index & andf.index
            for column in setcol:
                print(f'bgs {bgs_str} product {product} column = {column}')
                df.loc[ind, column] = andf.loc[ind, column]
        print(f'Записываю bgs{bgs_str}')
        df['newtime'] = df['newtime'] - pd.Timedelta(hours=3)
        df.to_csv(f'.//histdata//HistDatAll{bgs_str}.csv', index=False)
    return None


def get_filename(koren, bgs_id, suffics):
    return koren + '_' + bgs_id + '_' + suffics + '.csv'

def read_files(koren,bgs_id,start_suffics)->pd.DataFrame:
    """
    Читает файлы соответствующие маске и выдает итоговый датафрейм по всем файлам
    :param koren:
    :param bgs_id:
    :param start_suffics:
    :return:
    """
    filename_mask = get_filename(koren,bgs_id,start_suffics)
    df = read_files_from_mask(filename_mask)
    return df

def read_files_from_mask(filename_mask):
    df = pd.DataFrame()
    for filename in glob.glob(filename_mask):
        if os.path.exists(filename):
            print(f'Читаю файл = {filename}')
            start_df = pd.read_csv(filename)
            start_df['newtime'] = pd.to_datetime(start_df['newtime'])
            #print(f'start_df = {start_df.newtime.min()}, finish_df = {start_df.newtime.max()}')
            df = df.append(start_df, ignore_index=True, sort=False)
            #print(f'df_min = {df.newtime.min()}, df.max = {df.newtime.max()}')
            #print(f'columns = {df.columns}')
            print(f'df.shape = {df.shape}')

    df.drop_duplicates(subset=['newtime'], keep='last', inplace=True)
    df.sort_values('newtime', inplace=True)
    df.reset_index(drop=True, inplace=True)
    #print(f'start = {df.newtime.min()}, finish = {df.newtime.max()}')
    print(f'df.shape = {df.shape}')
    return df

def to_csv_files(df,koren,bgs_id, onlylast=True)->int:
    min_tm = df.newtime.min().replace(day=1,hour=0, minute=0, second=0, microsecond=0)
    max_tm = df.newtime.max().replace(day=1,hour=0, minute=0, second=0, microsecond=0)
    last_filename = get_filename(koren,bgs_id,max_tm.strftime("%Y%m"))
    print(f'min_month={min_tm},max_month={max_tm}, str = {df.newtime.max().strftime("%Y%m")}')
    while(min_tm<=max_tm):
        print(f'min_tm = {min_tm}')
        filename = get_filename(koren,bgs_id,min_tm.strftime("%Y%m"))
        next_tm = min_tm + dateutil.relativedelta.relativedelta(months=1)
        month_df = df[(df['newtime']>=min_tm) & (df['newtime']<next_tm)]
        print(f'month_df.shape = {month_df.shape}')
        min_tm = next_tm
        if onlylast:
            if min_tm >= max_tm:
                if min_tm == max_tm:
                    if os.path.exists(last_filename):
                        continue
                print(f'Write file = {filename}')
                month_df.to_csv(filename, index=False)
        else:
            print(f'Write file = {filename}')
            month_df.to_csv(filename, index=False)

    return 0