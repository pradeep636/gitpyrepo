import requests
from lxml import html
import pandas as pd
import stockList as sl
masterData='masterData.csv'
global dff,dff2

def main():
    global dff,dff2
    dff=downloadData()
    dff=dff[(dff['volume']>50)]
    
#    dff2=dff.groupby(['symbol','priceTrend']).priceTrend.agg('count').to_frame('priceTrendCount').reset_index() 
#    dff2=dff2[dff2['priceTrendCount']>=8]
#    set1=set(dff2['symbol'])
   
    dff2=dff[dff['percentChngLTP']>=50]
    dff2=dff2.sort_values(['percentChngLTP'],ascending=False).reset_index(drop=True)
    print(dff2)
    dff2.to_html('options-data.html')

def readData():
    df=pd.read_csv(masterData)
    return df

def downloadData():
    df=pd.DataFrame()
    for s in (sl.index):
        try:
            df=df.append(getOptionData(s),ignore_index=True)
        except Exception as e:
            print(e)
#    df.to_csv(masterData, sep=',', index=False)
    return df
    
def getOptionData(s):
    try:
        print(s)
        url='https://nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?symbolCode=-10006&symbol='+s.replace('&','%26')+'&symbol=NIFTY&instrument=-&date=-&segmentLink=17&symbolCount=2&segmentLink=17'
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        optionsChain = requests.get(url, headers=headers).content
    
        tree=html.fromstring(optionsChain)
        df=pd.DataFrame(columns=['callOI','callnetChngOI','callVolume','callnetChngLTP','strikePrice','putnetChngLTP','putVolume','putnetChngOI','putOI','callIV','putIV'])
        
        df.iloc[:,0]=tree.xpath('//table[@id="octable"]//tr/td[2]/text()')
        df.iloc[:,1]=tree.xpath('//table[@id="octable"]//tr/td[3]/text()')
        df.iloc[:,2]=tree.xpath('//table[@id="octable"]//tr/td[4]/text()')
        df.iloc[:,3]=tree.xpath('//table[@id="octable"]//tr/td[7]/text()')
        df.iloc[:,4]=tree.xpath('//table[@id="octable"]//tr/td[12]//text()')
        df.iloc[:,5]=tree.xpath('//table[@id="octable"]//tr/td[17]/text()')
        df.iloc[:,6]=tree.xpath('//table[@id="octable"]//tr/td[20]/text()')
        df.iloc[:,7]=tree.xpath('//table[@id="octable"]//tr/td[21]/text()')
        df.iloc[:,8]=tree.xpath('//table[@id="octable"]//tr/td[22]/text()')
        df.iloc[:,9]=tree.xpath('//table[@id="octable"]//tr/td[5]/text()')
        df.iloc[:,10]=tree.xpath('//table[@id="octable"]//tr/td[19]/text()')
        
        df_call=pd.DataFrame(columns=['strikePrice','callLTP'])
        df_call.iloc[:,0]=tree.xpath('//table[@id="octable"]//tr/td[6]/a/../../td[12]//text()')
        df_call.iloc[:,1]=tree.xpath('//table[@id="octable"]//tr/td[6]/a/text()')
        df=pd.merge(df,df_call[['strikePrice','callLTP']], on='strikePrice', how='left')
        
        df_put=pd.DataFrame(columns=['strikePrice','putLTP'])
        df_put.iloc[:,0]=tree.xpath('//table[@id="octable"]//tr/td[18]/a/../../td[12]//text()')
        df_put.iloc[:,1]=tree.xpath('//table[@id="octable"]//tr/td[18]/a/text()')
        df=pd.merge(df,df_put[['strikePrice','putLTP']], on='strikePrice', how='left')
        
        df['stockLTP']=str(tree.xpath('//table//span[1]//text()')[1]).split(' ')[1]
        df['date']=''.join(str(tree.xpath('//table//span[2]//text()')).split(' ')[3]).replace(',','')
        df['symbol']=s
        df['optionType']='NA'
        
        df.fillna('-', inplace=True)
        df=df.apply(lambda x: x.str.strip())
        df=df.applymap(lambda x: str(x.replace(',','')))
        df=df.applymap(lambda x: x if (str(x)!='-') else 0)
        
        df['OI']=0
        df['netChngOI']=0
        df['volume']=0
        df['LTP']=0
        df['netChngLTP']=0
        df['IV']=0
        df[['strikePrice','stockLTP','OI','netChngOI','volume','IV','LTP','netChngLTP','callOI','callnetChngOI','callVolume','callnetChngLTP','strikePrice','putnetChngLTP','putVolume','putnetChngOI','putOI','callLTP','putLTP','callIV','putIV']]=df[['strikePrice','stockLTP','OI','netChngOI','volume','IV','LTP','netChngLTP','callOI','callnetChngOI','callVolume','callnetChngLTP','strikePrice','putnetChngLTP','putVolume','putnetChngOI','putOI','callLTP','putLTP','callIV','putIV']].astype(float)
        
        pivot=0
        n=4
        for i in range(len(df)-1):
            if(df.loc[i,'stockLTP']>=df.loc[i,'strikePrice'] and df.loc[i+1,'stockLTP']<=df.loc[i+1,'strikePrice']):
                pivot=i
                break
        df=df.loc[pivot-n+1:pivot+n].reset_index(drop=True)
        
        for i in range(n):
            df.loc[i,'optionType']='PE'
            df.loc[i,'OI']=df.loc[i,'putOI']
            df.loc[i,'netChngOI']=df.loc[i,'putnetChngOI']
            df.loc[i,'volume']=df.loc[i,'putVolume']
            df.loc[i,'LTP']=df.loc[i,'putLTP']
            df.loc[i,'netChngLTP']=df.loc[i,'putnetChngLTP']
            df.loc[i,'IV']=df.loc[i,'putIV']
        columns = ['putOI','putnetChngOI','putVolume','putIV','putLTP','putnetChngLTP']
        df.drop(columns, inplace=True, axis=1)
        
        for i in range(n,n*2):
            df.loc[i,'optionType']='CE'
            df.loc[i,'OI']=df.loc[i,'callOI']
            df.loc[i,'netChngOI']=df.loc[i,'callnetChngOI']
            df.loc[i,'volume']=df.loc[i,'callVolume']
            df.loc[i,'LTP']=df.loc[i,'callLTP']
            df.loc[i,'netChngLTP']=df.loc[i,'callnetChngLTP']
            df.loc[i,'IV']=df.loc[i,'callIV']
        columns = ['callOI','callnetChngOI','callVolume','callIV','callLTP','callnetChngLTP']
        df.drop(columns, inplace=True, axis=1)
        
        df['optionTrend'] = df.apply(setOptionTrend, axis=1)   
        df['priceTrend'] = df.apply(setPriceTrend, axis=1)
        df['percentChngLTP']=round((df['LTP']-(df['LTP']-df['netChngLTP']))/(df['LTP']-df['netChngLTP'])*100,2)
        df['percentChngOI']=round((df['netChngOI']/df['OI'])*100,2)
        df=df[['symbol','stockLTP','strikePrice','optionType','OI','netChngOI','percentChngOI','volume','IV','LTP','netChngLTP','percentChngLTP','optionTrend','priceTrend']]
        
        return df
    except Exception as e:
        print(e)

def setOptionTrend(df):
    if(df['netChngOI']>0 and df['netChngLTP']>0):
        return 'longBuildup'
            
    elif(df['netChngOI']>0 and df['netChngLTP']<0):
        return 'shortBuildup'
        
    elif(df['netChngOI']<0 and df['netChngLTP']>0):
        return 'shortCovering'
        
    elif(df['netChngOI']<0 and df['netChngLTP']<0):
        return 'longLiquidation'
        
def setPriceTrend(df):
    if(df['optionType']=='CE'):
        if(df['optionTrend'] in ['longBuildup','shortCovering']):
            return 'UP'
        elif(df['optionTrend'] in ['shortBuildup','longLiquidation']):
            return 'DOWN'
        
    if(df['optionType']=='PE'):
        if(df['optionTrend'] in ['longBuildup','shortCovering']):
            return 'DOWN'
        elif(df['optionTrend'] in ['shortBuildup','longLiquidation']):
            return 'UP'

if __name__ == "__main__":
    main()