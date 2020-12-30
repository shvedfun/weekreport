import matplotlib.pyplot as plt
import matplotlib.widgets as wd
import matplotlib as mpl
import pandas as pd
import datetime

font = {'family': 'normal',
        'weight': 'bold',
        'size': 22}


class ManageActs():
    def __init__(self, ax, parent):
        self.parent = parent

    def forward(self, event):
        self.parent.change = True
        self.parent.start=self.parent.start + datetime.timedelta(minutes=int(self.parent.width // 3))
        return self.parent.start

    def backward(self, event):
        self.parent.change = True
        self.parent.start=self.parent.start - datetime.timedelta(minutes=int(self.parent.width // 3))
        return self.parent.start
    def wider(self, event):
        self.parent.change = True
        self.parent.width=self.parent.width + int(self.parent.width // 3)
        return self.parent.width
    def tighter(self, event):
        self.parent.change = True
        self.parent.width=self.parent.width - int(self.parent.width // 3)
        return self.parent.width

    def change_auto(self, event):
        self.parent.change = True
        self.parent.auto = not self.parent.auto
        return self.parent.auto

    def close(self, event):
        return plt.close(self.parent.fig)


class Show():
    def __init__(self, num_axes, start, width, figsize = [10, 6], button_show = True):
        self.change = True
        self.num_axes = num_axes
        self.fig, self.ax = plt.subplots(num_axes, figsize = figsize)
        self.fig.subplots_adjust(left = 0.04, right=0.98, top=0.95, bottom=0.04)
        if button_show:
            self.forward = plt.axes([0.85, 0.01, 0.07, 0.05])
            self.backward = plt.axes([0.7, 0.01, 0.07, 0.05])
            self.wider = plt.axes([0.30, 0.01, 0.07, 0.05])
            self.tighter = plt.axes([0.15, 0.01, 0.07, 0.05])
            self.aut = plt.axes([0.47, 0.01, 0.07, 0.05])
            self.close = plt.axes([0.05, 0.01, 0.04, 0.05])

            self.but_forward = wd.Button(self.forward, "Вперед")
            self.but_backward = wd.Button(self.backward, 'Назад')
            self.but_wider = wd.Button(self.wider, 'Шире')
            self.but_tighter = wd.Button(self.tighter, 'Уже')
            self.but_auto = wd.Button(self.aut, 'Auto')
            self.but_close = wd.Button(self.close, 'Close')

            self.ma = ManageActs(self.ax, self)
            self.but_forward.on_clicked(self.ma.forward)
            self.but_backward.on_clicked(self.ma.backward)
            self.but_wider.on_clicked(self.ma.wider)
            self.but_tighter.on_clicked(self.ma.tighter)
            self.but_auto.on_clicked(self.ma.change_auto)
            self.but_close.on_clicked(self.ma.close)

        self.plot_array = []
        self.scatter_array = []
        self.text_array = []

        self.start = start#datetime.datetime.strptime(start,'%Y-%m-%d %H:%M')
        self.width = width
        self.auto = False
        self.font = font
        #mpl.rc('font',**font)

    def pause(self, interval):
        return plt.pause(interval)

    def inc_start(self):
        self.start = self.start+datetime.timedelta(minutes=int(self.width/50))
        self.change=True

    def get_start(self):
        return self.start
    def get_width(self):
        return datetime.timedelta(minutes=self.width)

    def set_start(self, start):
        self.start = start
    def set_width(self,width):
        self.width=width

    def plot_old(self, num, x, y, label=None, color=None):
        return self.ax[num].plot(x, y, label=label, color=color)

    def add_plot(self, num, x, y, label=None, color=None):
        self.plot_array.append({'num':num,'x':x,'y':y,'label':label,'color':color})
        return self.plot_array

    def add_scatter(self,num, x, y, label=None, color=None):
        self.scatter_array.append({'num':num,'x':x,'y':y,'label':label,'color':color})
        return self.plot_array


    def change_plot(self, interval, loc = 6, legend=True):
        if self.change == True:
            self.clear()
            for value in self.plot_array:
                self.plot(value['num'],value['x'],value['y'],value['label'],value['color'])
            for value in self.scatter_array:
                self.scatter(value['num'],value['x'],value['y'],value['label'],value['color'])
            for value in self.text_array:
                self.text(value['num'],value['x'],value['y'],value['s'],**value['**kwargs'])
            self.grid()
            if legend:
                self.legend()
            self.change = False
        if self.auto:
            self.inc_start()
        plt.pause(interval)
        return self


    def fill_between(self, num, x, y, y0, facecolor=None, alpha=None):
        start,finish = self.get_startfinish(x)
        if start is not None:
            return self.ax[num].fill_between(x.loc[start:finish].values,y.loc[start:finish].values,y0,facecolor=facecolor,alpha=alpha)
        else:
            return None


    def add_text(self,num, x, y, s, **kwargs):
        self.text_array.append({'num':num,'x':x,'y':y,'s':s,'**kwargs':kwargs})
        return self.plot_array

    def text(self, num, x, y, s, **kwargs):
        return self.ax[num].text(x,y,s, **kwargs)

    def get_startfinish(self,x):
        start = self.get_start()
        finish = self.get_finish()
        if start<=x.min()<finish and start<x.max()<=finish:
            if start<x.min():
                start = x.min()
            if finish>x.max():
                finish = x.max()
            return start,finish
        if start>x.max() or finish<x.min():
            return None,None
        if start<x.min():
            start = x.min()
        if finish>x.max():
            finish=x.max()
        return start,finish

    def scatter(self, num, x, y, label=None, color=None):
        if x is None: return None
        start,finish = self.get_startfinish(x)
        #print(start, finish)
        if start is not None:
            return self.ax[num].scatter(x.loc[start:finish].values,
              y.loc[start:finish].values, color=color, label=label)
        else:
            return None



    def plot(self, num, x, y, label=None, color=None):
        if x is None: return None
        start,finish = self.get_startfinish(x)
        #print(start, finish)
        if start is not None:
            return self.ax[num].plot(x.loc[start:finish].values,y.loc[start:finish].values, color=color, label=label)
        else:
            return None

    def plot_vert_line(self, num, x,y,label = None, color = None):
        if x>=self.get_start() and x<=self.get_finish():
            return self.ax[num].plot((x,x),y, label=label, color=color)
        else:
            return None

    def get_finish(self):
        return self.get_start() + self.get_width()

    def clear(self,num=None):
        if num is None:
            for a in self.ax:
               res = a.clear()
        else:
            res=self.ax[num].clear()
        return res

    def legend(self, num=None):
        if num is None:
            for a in self.ax:
               res = a.legend()
        else:
            res=self.ax[num].legend()
        return res

    def grid(self, num=None):
        if num is None:
            for a in self.ax:
               res = a.grid()
        else:
            res=self.ax[num].grid()
        return res

    def show(self):
        plt.show(self.fig)

    def savefig(self, *args, **kwargs):
        return self.fig.savefig(*args, **kwargs)

if __name__ == '__main__':
    from adviser_new import *
    df = pd.read_csv('hist_data\\history_df_190301.csv')
    df['newtime']=pd.to_datetime(df['newtime'])
    df = BGS.prepare_mean(BGS, df, ['ANALOG\\WFIR21_1\\PV'])
    df['deltaW21_30'] = df['ANALOG\\WFIR21_1\\PV']-df['ANALOG\\WFIR21_1\\PV'].shift(30)
    df['deltaW21_60_30'] = df['ANALOG\\WFIR21_1\\PV'].shift(30)-df['ANALOG\\WFIR21_1\\PV'].shift(60)
    df['2deltaW21_30'] = df['deltaW21_30']-df['deltaW21_60_30']

    df['target_retur'] = df['ANALOG\FIRC841_1\PV'].rolling(60).mean() * 1.25 * 1.25
    df['coef'] = (df['ANALOG\\WFIR21_1\\PV']-df['target_retur'])*df['deltaW21_30']
    df['mean_retur'] = df['ANALOG\WFIR21_1\PV'].rolling(1600).mean()
    df['mean_4463'] = df['ANALOG\\PIRC4463_1\\PV'].rolling(1600).mean()
    df['mean_retur_4463'] = (6-df['mean_4463'])/df['mean_retur'] *10
    df.set_index('newtime', drop=False, inplace=True)
    print(df.head(5))

    #df_new = pd.read_csv('temp_data\\hist_pred_adv_lgbm.csv')
    #df_new['newtime']=pd.to_datetime(df_new['newtime'])
    #df_new['pred_adv_lgbm']=df_new['ANALOG\\WFIR21_1\\PV']
    #df_new.set_index('newtime',inplace=True)
    #df=df.join(df_new['pred_adv_lgbm'])

    #df_new = pd.read_csv('temp_data\\hist_pred_adv_ridge.csv')
    #df_new['newtime']=pd.to_datetime(df_new['newtime'])
    #df_new['pred_adv_ridge']=df_new['ANALOG\\WFIR21_1\\PV']
    #df_new.set_index('newtime',inplace=True)
    #df=df.join(df_new['pred_adv_ridge'])

    #df_new = pd.read_csv('temp_data\\hist_pred_asis_lgbm.csv')
    #df_new['newtime']=pd.to_datetime(df_new['newtime'])
    #df_new['pred_asis_lgbm']=df_new['ANALOG\\WFIR21_1\\PV']
    #df_new.set_index('newtime',inplace=True)
    #df=df.join(df_new['pred_asis_lgbm'])

    #df_new = pd.read_csv('temp_data\\hist_pred_asis_ridge.csv')
    #df_new['newtime']=pd.to_datetime(df_new['newtime'])
    #df_new['pred_asis_ridge']=df_new['ANALOG\\WFIR21_1\\PV']
    #df_new.set_index('newtime',inplace=True)
    #df=df.join(df_new['pred_asis_ridge'])

    #df.reset_index(drop=True, inplace=True)
    print(df.head(5))

    show = Show(2,4700,600)

    while True:
        if show.change==True:
            #print(df.loc[show.get_start():show.get_start() + show.get_width(), 'newtime'].values)
            #print('-----------------')
            #print(df.loc[show.get_start():show.get_start()+show.get_width(),'ANALOG\WFIR21_1\PV'].values)
            show.clear(0)
            show.clear(1)
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),
            #        ['ANALOG\WFIR21_1\PV']].values, color='blue',label='WFIR21')
            #show.plot(0, df['newtime'], df['ANALOG\WFIR21_1\PV'], color='blue', label='WFIR21')
            #show.plot(0, df['newtime'], df['target_retur'], color='green', label='target_retur')
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),'pred_adv_lgbm'].values, color='red',label='pred_adv_lgbm')
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),['pred_adv_ridge']].values, color='cyan',label='pred_adv_ridge')
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),['pred_asis_lgbm']].values, color='black',label='pred_asis_lgbm')
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),['pred_asis_ridge']].values, color='green',label='pred_asis_ridge')
            #show.plot(0,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),['target_retur']].values, color='gray',label='target_retur')
            #show.plot(1,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),
            #        ['ANALOG\PIRC4463_1\PV']].values, color='black',label='PIRC4463')
            #show.plot(1,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),
            #        ['mean_retur']].values, color='blue',label='mean_retur')
            #show.plot(1,df.loc[show.get_start():show.get_start()+show.get_width(),'newtime'].values,df.loc[show.get_start():show.get_start()+show.get_width(),
            #        ['mean_4463']].values, color='green',label='mean_4463')
            show.plot(1, df['newtime'], df['mean_retur_4463'], color='red', label='mean_retur_4463')
            #show.plot(1, df['newtime'], df['deltaW21_30']+df['2deltaW21_30'].rolling(60).mean(), color='red', label='deltaW21_30+2delta')
            #show.plot(1, df['newtime'], df['2deltaW21_30'].rolling(1).mean(), color='green', label='2delta')
            #show.plot(1, df['newtime'], df['deltaW21_30'], color='blue', label='deltaW21_30')
#            show.plot(1, df['newtime'], df['coef'], color='red', label='coef')
            #show.plot(0, df['newtime'], df['2deltaW21_30'].rolling(60).mean()*10, color='green', label='2deltaW21_30')

            show.change=False
            show.legend(0)
            show.legend(1)
            show.ax[0].grid(True)
            show.ax[1].grid(True)

            #print(f'show.get_start()={show.get_start()}')
            #print(f'show.get_width() = {show.get_width()}')
        #print(df.loc[show.get_start():show.get_start() + show.get_width(), 'newtime'])
        if show.auto:
            show.inc_start()
        show.change_plot(0.1)

    #df=df[df['ANALOG\WFIR21_1\PV']>3]
    #df=df[df['deltaW21_30']<=15]
    #df=df[df['coef']<=100]
    #fig, ax = plt.subplots(1,2)
    #ax[0].hist(df['deltaW21_30'], 100)
    #ax[1].hist( df['coef'],100)
    #plt.show(fig)