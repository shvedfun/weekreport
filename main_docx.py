import docxtpl
import datetime
import os
import advise_lib_crop as al
import pandas as pd
import numpy as np
import show2img as sh
import matplotlib.pyplot as plt

KOREN = 'fact'
DataDir = 'C:\\workspace\\UCAdviser\\Adviser\\Data\\'

sh_prop = {
    0: {'WFIR21': 'WFIR21', 'FIRC841': 'FIRC841', 'FIRC17': 'FIRC17', 'trg_retur': 'trg_retur'}
    , 1: {'PIRC4463_mean20': 'PIRC4463_mean20', 'ADV_PIRC4463': 'ADV_PIRC4463'} #'D61_H2O_FIRC841': 'D61_H2O_FIRC841',
    , 2: {'TIR811': 'TIR811','T811_mean720':'T811_mean720'}  # ,'TIR812':'TIR812'
    , 3: {'TIR812': 'TIR812', 'ADV_TIR812': 'ADV_TIR812','T812_mean720':'T812_mean720'}
    , 4: {'KS_2_5_min':'KS_2_5_min', 'KS_2_5':'KS_2_5_align', 'KS_2_5_OK':'KS_2_5_OK'}  #'FIRC841/Q':'FIRC841/Q' 'Q':'Q' 'FIRC17_T811': 'FIRC17_T811', 'F_T_H2O_F841':'F_T_H2O_F841','F_T_P_H2O_F841':'F_T_P_H2O_F841'
    #, 5: {'BGS_2_5_Dcp':'BGS_2_5_align'} #'D61_H2O_mn_align':'D61_H2O_mn_align', {'D61_H2O_align': 'D61_H2O_align'} , 'BGS_2':'BGS_2','BGS_6_3':'BGS_6_3','BGS_5_6':'BGS_5_6'
    }

def dynamic_table_tst():
    tpl = docxtpl.DocxTemplate('dynamic_table_tpl.docx')
    context = {
        'col_labels': ['Фрукт', 'vegetable', 'stone', 'thing'],
        'tbl_contents': [
            {'label': 'Желтый', 'cols': ['Бананssssssssssssssssssssssssssssss', 'capsicum', 'pyrite', 'taxi']},
            {'label': 'red', 'cols': ['apple', 'tomato', 'cinnabar', 'doubledecker']},
            {'label': 'green', 'cols': ['guava', 'cucumber', 'aventurine', 'card']},
        ],
    }
    tpl.render(context)
    tpl.save('dynamic_table.docx')
def read_df(filename, start_time, finish_time):
    #res_df = pd.read_csv(filename)
    res_df = al.read_files_from_mask(filename)
    res_df['newtime'] = pd.to_datetime(res_df['newtime'])
    res_df.set_index('newtime', drop=False, inplace=True)
    res_df = res_df.sort_index()
    if start_time is not None:
        res_df = res_df[res_df['newtime'] >= start_time]

    if finish_time is not None:
        res_df = res_df[res_df['newtime'] <= finish_time]
    return res_df

def add_analityc_align(res_df, param_dict):
    al_df = res_df.copy().dropna(how='any', subset=['KS_2_5', 'KS_Dcp', 'BGS_2_5'])
    al_df['newtime'] = al_df['newtime'].apply(lambda x: al.get_aligned_analityc_datetime(x))
    al_df.set_index('newtime', drop=False, inplace=True)
    al_df.drop_duplicates(subset='newtime', keep='last', inplace=True)
    al_df.set_index('newtime', drop=False, inplace=True)
    ind = al_df.index & res_df.index
    for key, val in param_dict.items():
        if key in al_df.columns:
            if key in ['D61_H20', 'D61_5_H2O', 'D61_6_H2O', 'D61_7_H2O']:
                al_df['D61_H2O_mn'] = al_df[['D61_H20', 'D61_5_H2O', 'D61_6_H2O', 'D61_7_H2O']].mean(axis=1)
                res_df.loc[ind, 'D61_H2O_mn_align'] = al_df.loc[ind, 'D61_H2O_mn']
                res_df['D61_H2O_mn_align'] = res_df['D61_H2O_mn_align'].interpolate()

            res_df.loc[ind, val] = al_df.loc[ind, key]
            res_df[val] = res_df[val].interpolate()
    return res_df

def render_doc(doc_tmpl, context, save_name ='report.docx'):

    doc_tmpl.render(context)
    doc_tmpl.save(save_name)
    return doc_tmpl

def read_data2dict(koren, start = None, finish = None):
    dict_res_df = {}
    for bgs_id in (1, 2, 3):
        print(f'BGS={bgs_id}')
        filename = os.path.abspath(DataDir + al.get_filename(koren, str(bgs_id), '*')) #_2001_full_191201
        print(f'filename = {filename}')
        res_df = read_df(filename, start, finish)

        print(f'res_df.max() = {res_df["newtime"].max()}')
        #print(f'res_df.columns = {res_df.columns}')
        #print(f'res_df.tail = {res_df.tail()}')
        dict_res_df[bgs_id] = {'res_df':res_df}

    filename = os.path.abspath(DataDir + al.get_filename(KOREN,str(0),'*'))  # _2001_full_191201
    analityc0_df = read_df(filename, start, finish)
    return dict_res_df, analityc0_df

def prepare_data(dict_res_df):
    for bgs_id, _ in dict_res_df.items():
        print(f'Show BGS={bgs_id}')
        res_df = dict_res_df[bgs_id]['res_df']
        res_df.set_index('newtime', drop=False, inplace=True)
        res_df = res_df.sort_index()
        for col_name in analityc0_df.columns:
            if col_name == 'newtime' or col_name == 'trg_retur':
                continue
            res_df[col_name] = analityc0_df[col_name]
        res_df.fillna(method='ffill', inplace=True)

        res_df = add_analityc_align(res_df, params_dict)
        tmp_df = res_df[(res_df['KS_2_5_align']>=95)].copy()
        res_df.loc[tmp_df.index,'KS_2_5_OK'] = tmp_df['KS_2_5_align']
        res_df['KS_2_5_min'] = 95
        res_df['KS_Dcp_min'] = 3.1
        res_df['KS_Dcp_max'] = 3.5

        tmp_df = res_df[(res_df['KS_Dcp_align'] <= 3.5) & (3.1 <= res_df['KS_Dcp_align'])].copy()
        res_df.loc[tmp_df.index, 'KS_Dcp_Ok'] = tmp_df['KS_Dcp_align']

        res_df['PIR16_mean60'] = np.abs(res_df['PIR16'].rolling(60, min_periods=1,center=True).mean())
        res_df['FIRC17_T811'] = res_df['FIRC17'].rolling(60,min_periods=1, center=True).mean() * res_df['TIR811'].rolling(60,min_periods=1, center=True).mean()

        res_df['PIRC4463_mean20'] = res_df['PIRC4463'].rolling(20,min_periods=1, center = True).mean()
        res_df['T811_mean720'] = res_df['TIR811'].rolling(720,min_periods=1).mean()
        res_df['T812_mean720'] = res_df['TIR812'].rolling(720,min_periods=1).mean()
        res_df['Delta_T'] = res_df['TIR811'] - res_df['TIR812']
        res_df['Q'] = (res_df['Delta_T'] * res_df['FIRC17']).rolling(15, min_periods=1).mean()
        res_df['FIRC841/Q'] = res_df['FIRC841']/res_df['Q']
        res_df['QFIRC841'] = res_df['Q']/(res_df['FIRC841'].rolling(60, min_periods=1).mean()*res_df['D61_H2O_mn_align'])

        dict_res_df[bgs_id] = {'res_df':res_df}

    return dict_res_df

def analisys4percent(res_df, params_limit, loc_dict):
    if 'ADV_PIRC4463' in res_df.columns:
        res_df, perc_PIRC4463 = al.estimate_correlation(res_df, 'PIRC4463', 'ADV_PIRC4463',
                                                     params_limit['PIRC4463'],
                                                     averaging_time=5)
        loc_dict['ADV_PIRC4463_perc'] = perc_PIRC4463
    if 'ADV_TIR812' in res_df.columns:
        res_df, perc_TIR812 = al.estimate_correlation(res_df, 'TIR812', 'ADV_TIR812', params_limit['TIR812'],
                                                   averaging_time=5)
        loc_dict['ADV_TIR812_perc'] = perc_TIR812
    if 'ADV_PIR16' in res_df.columns:
        res_df, perc_PIR16 = al.estimate_correlation(res_df, 'PIR16', 'ADV_PIR16', params_limit['PIR16'],
                                                  averaging_time=5)
        loc_dict['ADV_PIR16_perc'] = perc_PIR16
    return res_df, loc_dict

def add_all_img(context, dict_res_df, sh_prop):
    # Подготавливаю картинку
    showlist = {}
    for bgs_id, _ in dict_res_df.items():
        print(f'Show BGS={bgs_id}')
        res_df = dict_res_df[bgs_id]['res_df']
        print(f'res_df.columns = {res_df.columns}')
        res_df['newtime'] = res_df['newtime'] + pd.Timedelta(hours=3)
        res_df.set_index('newtime', drop=False, inplace=True)
        res_df = res_df.sort_index()

        show = sh.Show(len(sh_prop), start, int((finish - start) / datetime.timedelta(minutes=1)), figsize=[11, 6.5],
                       button_show=False)  # +pd.Timedelta(hours=3)

        showlist[bgs_id] = show
        show.fig.suptitle(f'БГС №{bgs_id}', fontsize=12)
        for key, prop in sh_prop.items():
            for name, col_name in prop.items():
                show.add_plot(key, res_df['newtime'], res_df[col_name], label=name)

    imgs = {}
    for bgs_id, _ in showlist.items():
        showlist[bgs_id].change_plot(0.1, legend=True)
        showlist[bgs_id].savefig('imgtmp_' + str(bgs_id) + '.png')  # , dpi = 200
        context['imgall_' + str(bgs_id)] = docxtpl.InlineImage(doc, 'imgtmp_' + str(bgs_id) + '.png')
    plt.close('all')
    return context

params_limit = {'PIRC4463': {'diff': 0.3,'time_diff': 15},'TIR812': {'diff': 2,'time_diff': 70},'PIR16': {'diff': 1,'time_diff': 20}}

params_dict = {'KS_2': 'KS_2_align', 'KS_2_5': 'KS_2_5_align', 'KS_Dcp': 'KS_Dcp_align', 'BGS_2_5': 'BGS_2_5_align',
               'D61_H2O': 'D61_H2O_align',
               'D61_PH': 'D61_PH_align', 'D61_5_H2O': 'D61_5_H2O_align', 'D61_6_H2O': 'D61_6_H2O_align',
               'D61_7_H2O': 'D61_7_H2O_align'}

parameter_properties = {'FIRC841':{'show':True,}
    , 'WFIR21':{'show':True,}
    , 'PIRC4463':{'show':True, 'adv_show':'ADV_PIRC4463'}
    , 'TIR812':{'show':True, 'adv_show':'ADV_TIR812'}
    ,  'PIR16':{'show':True, 'adv_show':'ADV_PIR16'}
    , 'TIR811':{'show':True,}
    , 'FIRC17':{'show':True,}
    , 'Delta_T':{'show':True,}
    , 'Q':{'show':True,}
    , 'QFIRC841':{'show':False,}
    ,}
conf = {'template_name': 'WeekReportTmplt.docx', 'weeks': {'week2020-11-02':{'start': '2020-11-02'}
    ,'week2020-11-09':{'start': '2020-11-09'}
    ,'week2020-11-16':{'start': '2020-11-16'}
    ,'week2020-11-23':{'start': '2020-11-23'}
    ,'week2020-11-30':{'start': '2020-11-30'}
    ,'week2020-12-07':{'start': '2020-12-07'}
    ,'week2020-12-14':{'start': '2020-12-14'}
    ,'week2020-12-21':{'start': '2020-12-21'}
    ,'NS2020-09-25':{'start': '2020-09-25', 'finish':'2020-12-04', 'name_report':'Отчет производства NS'}
    ,'IAS2020-12-05':{'start': '2020-12-05', 'finish':'2020-12-19', 'name_report':'Отчет производства ИАС'}
    ,}}

SKIP_EXIST = True

if __name__ == '__main__':

    for week, prop in conf['weeks'].items():
        otchet_file_name = os.getcwd()+'\\save\\'+week+'.docx' #'WeekReport'+str(start.year)+str(start.month)+str(start.day)
        if os.path.exists(otchet_file_name) and SKIP_EXIST:
            continue
        context = {}
        if 'name_report' in prop.keys():
            context['name_report'] = prop['name_report']
        else:
            context['name_report'] = 'Еженедельный отчет'
        start = datetime.datetime.strptime(prop['start'], '%Y-%m-%d')
        print(f'start = {start}')
        doc = docxtpl.DocxTemplate(conf['template_name'])
        context['start'] = start
        if 'finish' in prop.keys():
            finish = datetime.datetime.strptime(prop['finish'], '%Y-%m-%d')
        else:
            finish = start + datetime.timedelta(days=7) #datetime.datetime.strptime(conf['finish'], '%Y-%m-%d') - datetime.timedelta(minutes=1)
        context['finish'] = finish
        print(f'start = {start}, finish = {finish}')
        # Читаю и подготвавливаю данные
        dict_res_df, analityc0_df = read_data2dict(KOREN, start, finish)
        dict_res_df = prepare_data(dict_res_df)

        for bgs_id, _ in dict_res_df.items():
            print(f'Show BGS={bgs_id}')
            df = dict_res_df[bgs_id]['res_df']
            df = df[(df.index>=start)&(df.index<finish)]
            wrk_tm = df[df['FIRC841'] > 1]['FIRC841'].count()
            stop_tm = int((finish - start)/datetime.timedelta(minutes=1)) - wrk_tm
            context['worktime_' + str(bgs_id)] = str(int(wrk_tm/60)) + ' ч. ' + str(int(wrk_tm%60)) + ' мин.'
            context['stoptime_' + str(bgs_id)] = str(int(stop_tm/60)) + ' ч. ' + str(int(stop_tm%60)) + ' мин.'

            loc_dict = al.get_analisysfromdf(df, bgs_id, 'Весь период', start, finish, 5.)
            df, loc_dict = analisys4percent(df, params_limit, loc_dict)
            print(f'loc_dict = {loc_dict}')
            if 'Выпущено' in loc_dict.keys():
                context['production_' + str(bgs_id)] = round(loc_dict['Выпущено'], 2)
            else:
                context['production_' + str(bgs_id)] = 0.
            context['ADV_PIRC4463_perc_' + str(bgs_id)] = loc_dict['ADV_PIRC4463_perc']
            context['ADV_TIR812_perc_' + str(bgs_id)] = loc_dict['ADV_TIR812_perc']
            context['ADV_PIR16_perc_' + str(bgs_id)] = loc_dict['ADV_PIR16_perc']
            context['bad_2_5_' + str(bgs_id)] = round(loc_dict['Вып.отсева.2_5'],2)
            context['percent_bad_2_5_' + str(bgs_id)] = round(loc_dict['Вып.отсева.2_5']/loc_dict['Выпущено']*100.,2)
            context['bad_2_' + str(bgs_id)] = round(loc_dict['Вып.отсева.2'], 2)
            context['percent_bad_2_' + str(bgs_id)] = round(loc_dict['Вып.отсева.2']/loc_dict['Выпущено']*100., 2)

            df_tmp = df[df['FIRC841']>1.]
            for param, prop in parameter_properties.items():
                context[param+'_'+str(bgs_id)+'_mean'] = round(df_tmp[param].mean(),2)
                context[param+'_'+str(bgs_id)+'_std'] = round(df_tmp[param].std(),2)
                if prop['show']:
                    fig, ax = plt.subplots(1, figsize=[11,2])
                    fig.subplots_adjust(left=0.04, right=0.98, top=0.95, bottom=0.12)
                    ax.plot(df.index, df[param], label = param)
                    if 'adv_show' in prop.keys():
                        ax.plot(df.index, df[prop['adv_show']], label = prop['adv_show'])
                    ax.grid()
                    ax.legend()
                    plt.pause(0.1)
                    namefltmp = os.getcwd()+'\\tmp\\img_' + param + '_'+str(bgs_id) + '.png'
                    fig.savefig(namefltmp) #str(bgs_id)+
                    plt.close(fig)
                    context['img_' + param + '_' + str(bgs_id)] = docxtpl.InlineImage(doc, namefltmp)

        print(f'context = {context}')
        context = add_all_img(context, dict_res_df, sh_prop)
        render_doc(doc, context, otchet_file_name)


