#workable processing code around computer folder.
#Test on 8 profiles each (24 total)


from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.by import By
#https://medium.com/nerd-for-tech/linked-in-web-scraper-using-selenium-15189959b3ba

#https://www.us-proxy.org/
from bs4 import BeautifulSoup as bs
import requests
import random
import csv
import os
import re

from multiprocessing import Pool


#END oF IMPORTS
folder = "C:\\LinkedInScrapeProject\\"




#FUNCTION DEF

def liscrape(varlist):
    
    sleep(varlist[0])
    
    ctr = 0
    email = varlist[1][0]
    password = varlist[1][1]
    profcareer = []

    gurl = 'https://www.linkedin.com'
    
    
    pth = 'C:\\chromedriver.exe'
    
    randsleeper = {1:11.2,2:13.7,3:14.5,4:12.3,
                   5:14.9,6:13.1,7:11.8,8:15.3,
                   9:15.7,10:14,11:16.7,12:17,13:19,14:11,15:25,16:18.9,17:15,
                  18:11,19:13,20:15.7,21:14.4,22:12.4,23:16.3,24:8.8,25:10.1}
    chrome = webdriver.Chrome(pth)
    chrome.get("https://www.linkedin.com/login")
    sleep(2)
    chrome.find_element_by_id('username').send_keys(email)
    chrome.find_element_by_id('password').send_keys(password)
    chrome.find_element_by_id('password').send_keys(Keys.RETURN)
    sleep(25)
    
    for no in varlist[2]:
        chrome.get(gurl + no)
        sleep(3)
        
        #chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        try:
            css_sel = 'section.artdeco-card.ember-view.break-words.pb3'
            
            experience = [x for x in bs(chrome.page_source).select(css_sel) if x.find('div',{'id':'experience'})]
            
            # css selector for when you need to select an element with a space-separated class
            career = []
            #classes
            jtitle = 'mr1 t-bold'
            timeframe = 't-14 t-normal t-black--light'
            jtitlei = "mr1 hoverable-link-text t-bold"
            newcss = 'div.display-flex.flex-row.justify-space-between'
            #newcss = 'div.display-flex.flex-column.full-width'
            jobslist = experience[0].find('div',{'class':'pvs-list__outer-container'}).select(newcss)
            jobslist = [x for x in jobslist if not x.find('a',{'data-field':'experience_company_logo'})]
            for j in jobslist:
                templ = []
                for sp in j.find_all('span'):
                    if 'class' in sp.attrs:
                        if ' '.join(sp['class']) in [jtitle,timeframe,jtitlei]:
                            txtsp = sp.find('span',{'aria-hidden':'true'}).text
                            templ.append(txtsp)
                career.append(templ)
        except:
            career = ['ERROR']
        profcareer.append([no,career])
        sleep(1)
        chrome.get("https://www.linkedin.com/feed")
        sleep(int(randsleeper[random.randint(1,25)]/1))
        
        if career[0] == 'ERROR':
            print('ERROR ' + email)
    try:
        chrome.close()
    except:
        print('Quit Fail')
    return profcareer
    #return [['test',['ERROR']],['test1',['ERROR']]]
#END FUNCTIOn DEF






#burnersgroup = [1,2,3]

top = 80 # total to scrape across all 3 burner profs
divisor = 2 #- needs to be same as number of burner profs and as Pool variable(3)

folder = "C:\\LinkedInScrapeProject\\"



#print(burnersgroup)
outburners = [['user1@email.com','password1'],['user2@email.com','password2']]
print(outburners)

files = os.listdir(folder)
profsdone = [f for f in files if '_Profiles' in f]

errors = []
with open(folder + "ErrorProfiles.txt",'r',encoding='utf-8',errors='replace') as err:
    err.readline()
    err_rdr = csv.reader(err)
    for e in err_rdr:
        errors.append(e[0])
        
        
profsdonelst = []
for pf in profsdone:
    with open(folder + pf,'r',encoding='utf-8',errors='replace') as pff:
        pff.readline()
        drd = csv.reader(pff,delimiter='\t')
        for row in drd:
            try:
                profsdonelst.append(row)
            except:
                print(row)
        
proflinks = list(set([i[0] for i in profsdonelst]))

srclinks = []
with open(folder + "20220522_Master.txt",'r',encoding='utf-8',errors='replace') as src:
    src.readline()
    srcr = csv.reader(src,delimiter='\t')
    for row in srcr:
        srclinks.append(row)
        
        
profsrclinks = list(set([i[0] for i in srclinks if i[0] not in errors]))



remainder = top % divisor

if remainder != 0:
    top -= remainder

notdone = list(set(profsrclinks).difference(set(proflinks)))[:top]

increment = top/divisor
splitlist = []
for i in range(top):
    if i == 0 or i % increment == 0:
        splitlist.append(i)
print(splitlist)        
splits = []

for i,v in enumerate(splitlist):
    
    if not i == len(splitlist) - 1:
    
        splits.append(notdone[v:splitlist[i+1]])
    else:
        splits.append(notdone[v:])
        
delays = [6,14,22]        
        
workervariables = [[delays[i],outburners[i],splits[i]] for i in range(2)]


print('starting the scrape')
def main():
    with Pool(2) as p:
        results = p.map(liscrape, workervariables)
    
    flatresults = [sublist for i in results for sublist in i]
     
    for p in flatresults:
        res = p[1]
        if res != ['ERROR']:
            for r in res:
                if len(r) < 2:
                    p[1] = ['ERROR']
                    break      
    
    
    with open(folder + "20220509_Master_Profilesiii.txt",'a',newline='',encoding='utf-8',errors='replace') as latest:
        wrt = csv.writer(latest,delimiter='\t')
        for nd in flatresults:
            if nd[1] != ['ERROR']:
                lnk = nd[0]
                crr = nd[1]
                for i,v in enumerate(crr):
                    jb = v[0]
                    dur = v[1]
                    rw = [lnk,dur,jb,i]
                    wrt.writerow(rw)
    
    with open(folder + "ErrorProfiles.txt",'a',newline='',encoding='utf-8',errors='replace') as erro:
        wrt = csv.writer(erro,delimiter='\t')
        #wrt.writerow(['ErrorProfile'])
        for nd in flatresults:
            if nd[1] == ['ERROR']:
                rw = [nd[0]]
                wrt.writerow(rw)      
if __name__ == "__main__":
    main()