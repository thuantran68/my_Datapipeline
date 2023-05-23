import random
import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementNotInteractableException

def BighandMove(name):
    
    #Declare browser
    driver = webdriver.Chrome("chromedrive.exe")
    
    Datetudoanh, tudoanhBUY, tudoanhSELL, tongtudoanh = [], [], [], []
    Datekhoingoai, khoingoaiBUY, khoingoaiSELL, tongkhoingoai = [], [], [], []
    
    count=1
    #TU DOANH:
    URL='https://s.cafef.vn/Lich-su-giao-dich-'+name.upper()+'-7.chn#data'
    driver.get(URL)
    sleep(random.randint(5, 10))
        
    
    while count<2:
    
        #Take data
        elems=driver.find_elements(By.CSS_SELECTOR,".TuDoanDetailRow")
        tudoanh=[elem.text for elem in elems ]
        Datetudoanh=Datetudoanh+tudoanh[1::8]
        tudoanhBUY=tudoanhBUY+[float(i) for i in tudoanh[5::8]]
        tudoanhSELL=tudoanhSELL+[float(i) for i in tudoanh[6::8]]
        tongtudoanh=tongtudoanh+[float(i) for i in tudoanh[7::8]]
    
        elems=driver.find_elements(By.CSS_SELECTOR,".Item_DateItem")
        Datekhoingoai=Datekhoingoai+[elem.text for elem in elems]
    
        #Click button to next page:
        page_element='//*[@id="'+str(count+1)+'"]'
        driver.find_element('xpath',page_element).click() 
        sleep(random.randint(1,3)) 
        
        count+=1
    
    count=1 
    #KHOI NGOAI:
    URL='https://s.cafef.vn/Lich-su-giao-dich-'+name.upper()+'-3.chn#data'
    driver.get(URL)
    sleep(random.randint(5, 10))
    
    while count<2:  
    
        #Take data
        elems=driver.find_elements(By.CSS_SELECTOR,".Item_Price")
        khoingoai=[elem.text for elem in elems ]
        khoingoaiBUY=khoingoaiBUY+[round(int(i.replace(',', ''))/10**9,2) for i in khoingoai[4::9]]
        khoingoaiSELL=khoingoaiSELL+[round(int(i.replace(',', ''))/10**9,2) for i in khoingoai[6::9]]
        tongkhoingoai=tongkhoingoai+[round(int(i.replace(',', ''))/10**9,2) for i in khoingoai[1::9]]
        
        elems=driver.find_elements(By.CSS_SELECTOR,".Item_DateItem")
        Datekhoingoai=Datekhoingoai+[elem.text for elem in elems]
        
        #Click button to next page:
        if count<3:
            page_element='//*[@id="ContentPlaceHolder1_ctl03_panelAjax"]/div/div/div/table/tbody/tr/td['+str(count+20)+']/a'
            driver.find_element('xpath',page_element).click() 
            sleep(random.randint(1,3)) 
        else:
            page_element='//*[@id="ContentPlaceHolder1_ctl03_panelAjax"]/div/div/div/table/tbody/tr/td['+str(22)+']/a'
            driver.find_element('xpath',page_element).click() 
            sleep(random.randint(1,3))
            
        count+=1
        
    #ADD to dataframe
    df_tudoanh=pd.DataFrame(list(zip(Datetudoanh,tudoanhBUY,tudoanhSELL,tongtudoanh)),
                                columns=["Date","tudoanhBUY","tudoanhSELL","tongtudoanh"])
    df_khoingoai=pd.DataFrame(list(zip(Datekhoingoai,khoingoaiBUY,khoingoaiSELL,tongkhoingoai)),
                                  columns=["Date","khoingoaiBUY","khoingoaiSELL","tongkhoingoai"])
    
    df=df_khoingoai.merge(df_tudoanh,"outer",on='Date')
    
    df.to_csv('D:/Desktop/big_hand.csv')

def SupplyDemand(name):
    
    #Declare browser
    driver = webdriver.Chrome("chromedrive.exe")
    #RS(52W):
    URL='https://fwt.fialda.com/co-phieu/'+name.upper()+'/thongkegiaodich'
    
    driver.get(URL)
    sleep(random.randint(5, 10))
    
    #close advertise
    try:
        driver.find_element('xpath','/html/body/div[6]/div/div[2]/div/div[2]/button').click() 
        sleep(random.randint(1,3)) 
    except ElementNotInteractableException:
        pass
    
    #Take data
    RSI,DateRSI=[],[]
        
    col = driver.find_elements(By.TAG_NAME, "td")
    DateRSI=[i.text for i in col[1::13]]
    RSI=[i.text for i in col[12::13]]
    df_RSI=pd.DataFrame(list(zip(DateRSI,RSI)),columns=["Date","RSI(52M)"])
    
    #Take supply and demand data:
    #change to supply and demand page:
    try:
        driver.find_element('xpath','/html/body/div[1]/div[1]/div[2]/div[4]/div/main/div/div[2]/div/div[2]/div/div[1]/div/ul/li[3]/a').click() 
        sleep(random.randint(1,3)) 
    except ElementNotInteractableException:
        pass
    
    #Take data
    DateSD,supply,demand,exchanged=[],[],[],[]
        
    col = driver.find_elements(By.TAG_NAME, "td")
    DateSD=[i.text for i in col[1::10]]
    supply=[int(i.text.replace(',', '')) for i in col[3::10]]
    demand=[int(i.text.replace(',', '')) for i in col[6::10]]
    exchanged=[i.text for i in col[8::10]]
    df_SD=pd.DataFrame(list(zip(DateSD,supply,demand,exchanged)),columns=["Date","supply","demand","exchanged"])
    
    df_supplydemand=df_SD.merge(df_RSI,on="Date",how="outer")
    
    df_supplydemand.to_csv('D:/Desktop/supply_demand.csv')
    
try:
    BighandMove("SSI")
    SupplyDemand("SSI")
except:
    print("WRONG STOCK NAME")

    
