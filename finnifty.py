## Finding the opening strike for Banknifty for data saving
import datetime
import sqlite3



qRes = api.get_quotes('NSE', 'Nifty Fin Service')

indexLtp=float(qRes['o'])
import math
mod = int(indexLtp)%50
if mod < 25:
    openstrike =  int(math.floor(indexLtp / 50)) * 50
else:
    openstrike=  int(math.ceil(indexLtp /50)) * 50
strike=[]
expiry='10OCT23'
for i in range(20):
    if i!=0:
        strike.append(f"FINNIFTY{expiry}C{openstrike-50*i}")
        strike.append(f"FINNIFTY{expiry}P{openstrike-50*i}")   
        strike.append(f"FINNIFTY{expiry}C{openstrike+50*i}") 
        strike.append(f"FINNIFTY{expiry}P{openstrike-50*i}")   
        #print(openstrike-100*i,openstrike+100*i)
    else:

        strike.append(f"FINNIFTY{expiry}P{openstrike}")   
        strike.append(f"FINNIFTY{expiry}C{openstrike}")   
strike

#Calculated Start Time create a datetime object
# convert the datetime object to a timestamp

#lastBusDay = datetime.datetime.today()
#one_day = datetime.timedelta(days=1)
#endtime_1=lastBusDay-one_day 
#endtime=ed,
tod=datetime.datetime.today()  
dt = datetime.datetime( tod.year, tod.month-1,  tod.day+15, 9, 15, 0) 
ts = dt.timestamp()


dt = datetime.datetime( tod.year, tod.month,  tod.day+1, 9, 15, 0) 
ed = dt.timestamp()  



# create a database connection
conn = sqlite3.connect('Finnifty.db')

for i in strike:
        df=pd.DataFrame(api.get_time_price_series(exchange="NFO",token=i,starttime=ts,endtime=ed,interval=1))  
        if df['stat'].unique()[0]=='Ok':
            df.drop(columns=['stat'],inplace=True)
            date_format = '%d-%m-%Y %H:%M:%S'
            df['time']=pd.to_datetime(df['time'], format=date_format)
         #,'intoi','intv','oi','v' 
            df[['into', 'intc','inth','intl','intv']] = df[['into', 'intc','inth','intl','intv']].astype(float)
            df[['intoi','intv','oi','v','intvwap']] = df[['intoi','intv','oi','v','intvwap' ]].astype(float)
            df['Strike']=i
            df['expiry']=expiry
            df.to_sql('FINNIFTY_OPTION_Onemin_new', conn, if_exists='append', index=False)
        else:
            print('Data Not Available')
    
   
    

#print(openstrike


# close the database connection
conn.close()







      
