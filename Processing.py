import csv
import re

####################
#####Edit to the folder where the ray data file is saved#####
####################
root = 'C:\\UCOVI\\LinkedInScrapeProject'




profsdict = {}
timings = []
jobtitles = []
with open(root + "\\20230821_LinkedInDataforPythonAnon.txt",encoding='ISO-8859–1') as f:
    f.readline()
    rdr = csv.reader(f,delimiter="\t")
    for row in rdr:
        link =row[0]
        duration = row[1]
        JobTitle = row[2]
        _rc = row[3]
        gn = row[4]
        gnl = row[5]
        job = {"duration":duration,"role":JobTitle,"nthlatest":int(_rc)}
        if link not in profsdict:
            profsdict[link] = {}
            profsdict[link]['jobs'] = [job]
            profsdict[link]['Group'] = gn
            profsdict[link]['GroupLink'] = gnl
        else:
            profsdict[link]['jobs'].append(job)
        timings.append(duration)
        jobtitles.append(JobTitle)
        

def timingsparse(x):
    stepone = x.replace('\xb7',"|").replace('\xc2',"|").replace("||","|")
    if len(re.findall("\|",stepone))==1:
        
        elems = stepone.split("|")
        duration = elems[1]
        yrs, ms = 0,0
        if duration == " Less than a year":
            months = 7 #take 7 as a ball park avg for anyone who was there less than 1 y (only 20 reocrds)
        else:
            if re.search("^\s*([0-9]{1,2})\s*yr[s\s]",duration):
                yrs = int(re.search("^\s*([0-9]{1,2})\s*yr[s\s]",duration).group(1))
            if re.search("([0-9]{1,2})\smo[s\s]*$",duration):
                ms = int(re.search("([0-9]{1,2})\smo[s\s]*$",duration).group(1))
            months = max((int(yrs*12) + ms -1),1) # taking a month off becuase LinkedIn overcounts
            
        return (elems[0], months)
        
    else:
        if re.search("20[0-9]{2}|19[0-9]{2}|-[0-9]{2}$",stepone): #its a plausible date - assign one month
            months = 1
        else:
            months = None
        return (stepone, months)
    

for p in profsdict:
    jb = profsdict[p]['jobs']
    for j in jb:
        tp,tn = timingsparse(j['duration'])

        j['duration'] = tp
        j['tenure_months'] = tn
        
        
suppressions = []
for p in profsdict:
    jb = profsdict[p]['jobs']
    for j in jb: 
        if j['tenure_months'] is None:
            suppressions.append(p)

profsdict = {p:profsdict[p] for p in profsdict if p not in suppressions}

def job_category(x,lookup):
    stepone = x.lower()
    stepone = re.sub('\s+',' ',stepone)
    targ = None
    for l in lookup:
        if l[1] == '':
            #print(l[0])
            if re.search(l[0],stepone):
                targ = l[2]
                break
            
        else:
            #print(l[0] + l[1])
            if re.search(l[0],stepone) and re.search(l[1],stepone):
                targ= l[2]
                break   
    if targ is None:
        targ = 'Other Job Role NEC'
    return targ



jobcats = [['(\W|^)analytics(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)data analyst(\W|$)', '(\W|^)intern(\W|$|s)|internship', 'Data Analytics Internship'],
['(\W|^)database analyst(\W|$)', '(\W|^)intern(\W|$|s)|internship', 'Data Analytics Internship'],
['(\W|^)insights* analyst(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)bi analyst(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)data analy', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)business intelligence', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)business intelligence analyst(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)crm analyst(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Analytics Internship'],
['(\W|^)data science(\W|$)', '(\W|^)intern(\W|$|s)', 'Data Science Internship'],
           
['(\W|^)engineer(ing|\W|$)|(\W|^)i[\.\s]{0,2}t(\W|$)|(\W|^)software(\W|$)',
 '(\W|^)intern(\W|$|s)', 'Other Technical Internship'],
           
['(\W|^)data analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],
['(\W|^)insights analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],
['(\W|^)bi analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],
['(\W|^)reporting analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],

['(\W|^)business intelligence analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],
['(\W|^)crm analyst(\W|$)', '(\W|^)sr(\W|$)|(\W|^)senior(\W|$)|(\W|^)principal(\W|$)|(\W|^)lead(\W|$)', 'Data Analyst - Senior Level'],
['(\W|^)data analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)insights analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)bi analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)reporting analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)business intelligence analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)crm analyst(\W|$)', '(\W|^)jr(\W|$)|(\W|^)junior(\W|$)|(\W|^)trainee(\W|$)', 'Data Analyst - Junior Level'],
['(\W|^)business development(\W|$)', '', 'Marketing/Sales'],
['(\W|^)data science(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Data science Leadership'],
['(\W|^)analytics(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Data Analytics Leadership'],
['(\W|^)data[\s\W]{1,3}analy', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Data Analytics Leadership'],
['(\W|^)data(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Data Management leadership'],
['(\W|^)software(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)web(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)devops(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)i[\.\s]{0,2}t(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)systems(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)information technology(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)development(\W|$)', '(\W|^)manager(\W|$)|(\W|^)team lead(\W|$)|(\W|^)head of(\W|$)', 'Software/IT Leadership'],
['(\W|^)data analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)insights analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)bi analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)reporting analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)business intelligence analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)crm analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)database analyst(\W|$)', '', 'Data Analyst - No stated seniority/mid-level'],
['(\W|^)sql developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)tableau developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)power bi developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)database developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)msbi developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)data analytics(\W|$)', 'engineer(\W|$)', 'BI/Database Developer'],
['(\W|^)b[\.\s]{0,2}i developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)business intelligence developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)oracle developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)salesforce developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)engineer .*insights(\W|$)', '', 'BI/Database Developer'],
['(\W|^)data engineer(\W|$)', '', 'Data Engineer'],
['(\W|^)data.+engineer(\W|$)', '', 'Data Engineer'],
['(\W|^)data scientist(\W|$)', '', 'Data Scientist'],
['(\W|^)decision scientist(\W|$)', '', 'Data Scientist'],
['(\W|^)machine learning(\W|$)', '', 'Data Scientist'],
['(\W|^)statistician(\W|$)', '', 'Statistician'],
['(\W|^)c[\.\s]{0,2}d[\.\s]{0,2}o(\W|$)', '', 'Chief Data Officer'],
['(\W|^)chief data officer(\W|$)', '', 'Chief Data Officer'],
['(\W|^)chief analytics officer(\W|$)', '', 'Chief Analytics Officer'],
['(\W|^)i[\.\s]{0,2}t support(\W|$)', '', 'IT'],
['(\W|^)i[\.\s]{0,2}t technician(\W|$)', '', 'IT'],
['(\W|^)systems administrator(\W|$)', '', 'IT'],
['(\W|^)i[\.\s]{0,2}t administrator(\W|$)', '', 'IT'],
['(\W|^)i[\.\s]{0,2}t manager(\W|$)', '', 'IT'],
['(\W|^)information technology(\W|$)', '', 'IT'],
['(\W|^)network administrator(\W|$)', '', 'IT'],
['(\W|^)i[\.\s]{0,2}t .* engineer(\W|$)', '', 'IT'],
['(\W|^)systems analyst(\W|$)', '', 'IT'],
['(\W|^)software engineer(\W|$)', '', 'Software Development'],
['(\W|^)software developer(\W|$)', '', 'Software Development'],
['(\W|^)web .*developer(\W|$)', '', 'Software Development'],
['(\W|^)backend developer(\W|$)', '', 'Software Development'],
['(\W|^)java .*developer(\W|$)', '', 'Software Development'],
['(\W|^)web engineer(\W|$)', '', 'Software Development'],
['(\W|^)software programmer(\W|$)', '', 'Software Development'],
['(\W|^)computer programmer(\W|$)', '', 'Software Development'],
['(\W|^)blockchain developer(\W|$)', '', 'Software Development'],
['(\W|^)devops(\W|$)', '', 'Software Development'],
['(\W|^)blockchain engineer(\W|$)', '', 'Software Development'],
['(\W|^)application developer(\W|$)', '', 'Software Development'],
['(\W|^)c[\.\s]{0,2}t[\.\s]{0,2}o(\W|$)', '', 'Chief Technology Officer'],
['(\W|^)chief technology officer(\W|$)', '', 'Chief Technology Officer'],
['(\W|^)business analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)risk analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)financial analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)credit analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)fraud analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)marketing(\W|$)', '', 'Marketing/Sales'],
['(\W|^)marketer(\W|$)', '', 'Marketing/Sales'],
['(\W|^)sales(\W|$)', '', 'Marketing/Sales'],
['(\W|^)h[\.\s]{0,2}r(\W|$)', '', 'HR'],
['(\W|^)human resources(\W|$)', '', 'HR'],
['(\W|^)people(\W|$)', '', 'Recruitment'],
['(\W|^)talent(\W|$)', '', 'Recruitment'],
['(\W|^)recruit(m|\W|$)', '', 'Recruitment'],
['(\W|^)executive search(\W|$)', '', 'Recruitment'],
['(\W|^)project manag', '', 'Project Management'],
['(\W|^)dba(\W|$)', '', 'DBA'],
['(\W|^)database administrator(\W|$)', '', 'DBA'],
['(\W|^)accountan', '', 'Accountancy/Financial Services'],
['(\W|^)audit', '', 'Accountancy/Financial Services'],
['(\W|^)insurance', '', 'Accountancy/Financial Services'],
['(\W|^)test(\W|$|ing|er)', '', 'Software Testing'],
['(\W|^)qa(\W|$)', '', 'Software Testing'],
['(\W|^)professor(\W|$)', '', 'Academia'],
['(\W|^)phd candidate(\W|$)', '', 'Academia'],
['(\W|^)msc researcher(\W|$)', '', 'Academia'],
['(\W|^)masters(\W|$)', '', 'Academia'],
['^student | student$|^student$', '', 'Academia'],
['(\W|^)trainer(\W|$)', '', 'Training'],
['(\W|^)teacher(\W|$)', '', 'Teaching'],
['(\W|^)intern(\W|$|s)', '', 'Other Internship'],
#ROUND 2 ADDED
['(\W|^)system[s\s]engineer(\W|$|s)', '', 'IT'],
['(\W|^)full stack developer(\W|$)', '', 'Software Development'],
['(\W|^)python developer(\W|$)', '', 'Software Development'],
['(\W|^)frontend developer(\W|$)', '', 'Software Development'],
['(\W|^)php developer(\W|$)', '', 'Software Development'],
['(\W|^)etl developer(\W|$)', '', 'Data Engineer'],
['(\W|^)i[\.\s]{0,2}t consu', '', 'IT'],
['(\W|^)i[\.\s]{0,2}t analyst', '', 'IT'],
['(\W|^)technical lead', '', 'Software/IT Leadership'],
['(\W|^)lecturer(\W|$)', '', 'Academia'],
['(\W|^)pricing analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)supply chain analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)product owner(\W|$)', '', 'Product Management'],
['(\W|^)product manager(\W|$)', '', 'Product Management'],
['(\W|^)head of product management(\W|$)', '', 'Product Management'],
['(\W|^)it assistant(\W|$)', '', 'IT'],
['data|business intelligence|(\W|^)bi(\W|$)|(\W|^)analytics(\W|$)|', 'consultant|consulting|freelance', 'Data Consultant'],
['(\W|^)analyst(\W|$)', '', 'Business Analyst'],
['(\W|^)sr data analytics(\W|$)', '', 'Data Analyst - Senior Level'],
['(\W|^)application developer(\W|$)', '', 'Software Development'],
['(\W|^)wordpress developer(\W|$)', '', 'Software Development'],
['^developer$', '', 'Software Development'],
['^programmer$', '', 'Software Development'],
['(\W|^)business intelligence engineer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)analytics engineer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)vba developer(\W|$)', '', 'BI/Database Developer'],
['(\W|^)it specialist(\W|$)', '', 'IT'],
['(\W|^)data specialist(\W|$)', '', 'Data Analyst - Senior Level'],
['(\W|^)reporting specialist(\W|$)', '', 'Data Analyst - Senior Level'],
['(\W|^)data entry(\W|$)', '', 'Data Analyst - Junior Level'],
['(\W|^)data administrator(\W|$)', '', 'Data Analyst - Junior Level'],
[' tutor$', '', 'Training'],
[' instructor$', '', 'Training'],
['(\W|^)instructor(\W|$)', '', 'Training'],
['academic staff', '', 'Academia'],
['academic researcher', '', 'Academia'],
['(\W|^)engineer(\W|$)', '', 'Other Engineering Role']
          ]




TLJobCatMap = {
'Academia': 'Academia/Education',
'Accountancy/Financial Services': 'Other Roles',
'BI/Database Developer': 'Data Engineering/Development',
'Business Analyst': 'Business Analyst',
'Chief Analytics Officer': 'Data Leadership',
'Chief Technology Officer': 'Tech Leadership',
'Data Analyst - Junior Level': 'Data Analytics',
'Data Analyst - No stated seniority/mid-level': 'Data Analytics',
'Data Analyst - Senior Level': 'Data Analytics',
'Data Analytics Internship': 'Internship',
'Data Analytics Leadership': 'Data Leadership',
'Data Consultant': 'Consultancy',
'Data Engineer': 'Data Engineering/Development',
'Data Management leadership': 'Data Leadership',
'Data Science Internship': 'Internship',
'Data science Leadership': 'Data Leadership',
'Chief Data Officer': 'Data Leadership',
'Data Scientist': 'Data Science',
'DBA': 'Data Engineering/Development',
'HR': 'Other Roles',
'IT': 'Dev/IT',
'Marketing/Sales': 'Other Roles',
'Other Internship': 'Internship',
'Other Job Role NEC': 'Other Roles',
'Other Technical Internship': 'Internship',
'Product Management': 'Other Roles',
'Project Management': 'Other Roles',
'Recruitment': 'Other Roles',
'Software Development': 'Dev/IT',
'Software Testing': 'Dev/IT',
'Software/IT Leadership': 'Tech Leadership',
'Statistician': 'Data Analytics',
'Teaching': 'Academia/Education',
'Training': 'Academia/Education',
'Other Engineering Role': 'Other Engineering Role'}



for p in profsdict:
    jb = profsdict[p]['jobs']
    for j in jb:
        jc = job_category(j['role'],jobcats)

        j['JobCategory'] = jc
        j['JobCategoryParent'] = TLJobCatMap[jc]
        
djs = 'Data Analytics'
profsdict_da = {}

for p in profsdict:
    flag = False
    jb = profsdict[p]['jobs']
    for j in jb:
        if j['JobCategoryParent'] == djs:

            flag=True
            break
    if flag:
        profsdict_da[p] = profsdict[p]
        
suppressions_da = []
for pa in profsdict_da:
    paj = profsdict_da[pa]['jobs']
    datajobs = [x for x in paj if x['JobCategoryParent']==djs] #jobs that are data
    datajobs_proper = len([x for x in datajobs if not re.search('data entry',x['role'].lower())])
    if len(datajobs) > 0 and datajobs_proper < 1:
        suppressions_da.append(pa)
    #print(len(datajobs))
    #print(datajobs_proper)


profsdict_da = {pda:profsdict_da[pda] for pda in profsdict_da if pda not in suppressions_da}


da_tenures_complete = []
da_profsunique = []
for p in profsdict_da:
    jlist = profsdict_da[p]['jobs']
    jlistredact = [j for j in jlist \
                if j['JobCategoryParent'] in ['Data Analytics','Data Science']]
    if len(jlistredact) > 0:
        da_profsunique.append(p)
    da_tenures_complete.extend(jlistredact)
    
    
def funneltenure(x):
    years = [1,0,0,0,0,0,0] #0y,1y,2y,3y,4y,5+y,10+y
    if x >= 12:
        years[1] = 1
    if x >= 24:
        years[2] = 1
    if x >= 36:
        years[3] = 1
    if x >= 48:
        years[4] = 1
    if x >= 60:
        years[5] = 1
    if x >= 120:
        years[6] = 1
    return years

with open(root + "\\CompletedAnalystTenures.csv",'w',newline='',encoding='utf-8') as out:
    wrt = csv.writer(out)
    headers = ['duration','nthlatest','role',
               'tenure_months','JobCategory','Situation',
              'All','At least 1yr','At least 2yrs','At least 3yrs',
               'At least 4yrs','At least 5yrs','At least 10yrs']
    wrt.writerow(headers)
    for rw_ in da_tenures_complete:
        if rw_['nthlatest'] < 1 or 'present' in rw_['duration'].lower():
            sitch = 'Present Role'
        else:
            sitch = 'Left job'
        rwdata = [rw_['duration'],rw_['nthlatest'],rw_['role'],rw_['tenure_months'],rw_['JobCategory'],sitch]
        yearsten = funneltenure(rw_['tenure_months'])
        rwdata.extend(yearsten)
        wrt.writerow(rwdata)