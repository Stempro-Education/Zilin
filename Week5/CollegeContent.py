import re
import spacy 
from spacy import displacy
import en_core_web_sm
from pathlib import Path
from IPython.display import display, Markdown, Latex 
import sys
import csv
import pandas as pd
import numpy as np
import os
from os import path
from datetime import datetime
from urllib.parse import urlparse, urljoin
import requests
import ntpath
from bs4 import BeautifulSoup, Comment
import time
from pprint import pprint 
import shutil
#!pip install tldextract
import tldextract

'''
    c=CollegeCrawl('University of Washington', 'https://www.washington.edu')
    c.GetAllUrls()
'''
class CollegeCrawl():
    Gap_Insecond=1
    Max_Pages=15      
    """
        collegename: name
        rooturl: https://www.university.edu. Scheme and netloc need to be complete
        prioritykeywords: ['apply','adimission'...] etc. if None then everth page 
        respectrobottxt: True
        use tldextract instead of urlparse to get the domain name. --changed on Jan 4 2020
    """
    def __init__(self,_collegename, _rooturl, 
                 _prioritykeywords=['apply', 'admission', 'application', 'deadline'], 
                 _url_file=None,                  
                 _save_to_folder=None,
                 _existingurlfile=None, #csv files that were visited in the past
                 _respectrobottxt=True, 
                _headers={'User-Agent':'Mozilla/5.0'} ):
        self.college=_collegename
         
        if urlparse(_rooturl).scheme=="":
            print('URL needs to have scheme. Please try again.')
            raise Exception('URL needs to have a scheme')          
        
        self.rootUrl=_rooturl
        #self.base_domain=urlparse(self.rootUrl).netloc
        self.base_domain=tldextract.extract(self.rootUrl).domain
        self.scheme=urlparse(self.rootUrl).scheme
        self.priorityKeywords=_prioritykeywords
        self.respectRobottext=_respectrobottxt
        if _save_to_folder==None or path.isdir(_save_to_folder)==False:
            self.save_to_folder=os.getcwd()
        else:
            self.save_to_folder=_save_to_folder
            
        # to make it less 
        if _existingurlfile==None or path.exists(_existingurlfile)==False:      
            self.existingurlfile=path.join(self.save_to_folder,re.sub(r"\s+", "_", self.college.strip()+'.csv'))
        else:
            self.existingurlfile=_existingurlfile
        self.allurls={}
        self.headers=_headers
        self.files=[]
            
    '''simple description'''        
    def __str__(self):
        return '{}. Starting URL: {}'.format(self.college, self.rootUrl)
    '''
        load _existingurlfile: two columns -- Url and status_code
        minimum assumptions: first two columns are url and status_code
    '''
    def Load_DiscoveredUrls(self, delimiter='\t', hasHeader=False, header_names=['url', 'status_code']):   
        if self.existingurlfile==None:
            return dict()
        else:
            if path.exists(self.existingurlfile) and re.sub(r'\s+', '_', self.college) \
                in ntpath.basename(self.existingurlfile):
                df_urls=pd.read_csv(self.existingurlfile,  delimiter=delimiter, names=['url', 'status_code'])
                df_urls=df_urls[df_urls['url']!='url']  
                return dict(zip(df_urls['url'], df_urls['status_code'])) #format: url:status_code. i.e., url is the key
            else:
                return dict() 
            
    """
        get all urls starting from rootUrl 
        headers={'User-Agent':'Mozilla/5.0'}
        #Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36
        #full list: https://www.whatismybrowser.com/guides/the-latest-user-agent/chrome
        response= requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text)  
        
        it will first load which ever has alread visited 

    """    
    def GetAllUrls(self, headers=None, links_only=True): 
        #load existing urls if any
        urls=self.Load_DiscoveredUrls() 
        
        if len(urls)==0:
            urls={self.rootUrl:'0'}  
            unvisited=[self.rootUrl]
        else:
            unvisited=[url for url, status_code in urls.items() if str(status_code)=='0'] 
            
            if len(unvisited)==0:
                unvisited=[self.rootUrl] 
            else:
                unvisited=sorted(unvisited, key=lambda item: (sum([w in item for w in self.priorityKeywords])*10+10)/len(item))
                
        if headers==None: 
            if self.headers:
                headers=self.headers
            else: 
                 headers={'User-Agent':'Mozilla/5.0'}  
       
        #get base_domain    
        if self.base_domain==None:
            self.base_domain=urlparse(unvisited[0]).netloc
        if self.scheme==None:
            self.scheme=urlparse(unvisited[0]).scheme
       
        pages_visited =0
         
        try:  
            while len(unvisited)>0:        
                pages_visited+=1
                url=unvisited.pop()    
                url_parsed=urlparse(url)
                url_domain_path='{uri.scheme}://{uri.netloc}'.format(uri=url_parsed)
              
                response=requests.get(url, headers=headers)     
                status_code=response.status_code
                
                urls[url]=status_code 
                 
                if status_code==200: 
                    soup=BeautifulSoup(response.text,  'lxml') #'html.parser')  
                    for link in soup.find_all('a'):  
                        if link.has_attr('href'):
                            link_url=link['href']    
                            
                            if not re.match('.+@.+', link_url ):
                                link_url=re.sub(r'[\/_+!@#$?\\\s]+$', '', link_url)

                                parsed_uri_path=urlparse(link_url) 
                                extract_uri_domain=tldextract.extract(link_url)                            
                                #parts: parsed_uri.domain + '.' + parsed_uri.suffix  
                                absolute_url=''                                 
                                if  (extract_uri_domain.domain=='') and re.match(r'^\/.*\w$', parsed_uri_path.path) :
                                    absolute_url=urljoin(url_domain_path,parsed_uri_path.path)
                                elif extract_uri_domain.domain==self.base_domain: # and re.match('^http', parsed_uri.scheme):
                                    if re.match(r'^\/\/', link_url):
                                        absolute_url=self.scheme+':'+link_url
                                    else:
                                        absolute_url=link_url
                                else:
                                    continue    

                                if absolute_url!='' and absolute_url not in urls:   
                                     urls[absolute_url]='0' 
                    self.SaveToCsv_FromResponse(url, response)  
              
                if pages_visited>=self.Max_Pages:
                    break   
                
                unvisited=[name for name, code in urls.items() if str(code)=='0']    
                #sorting rule: has keywords, short url, else
                unvisited=sorted(unvisited, key=lambda item: (sum([w in item for w in self.priorityKeywords])*10+10)/len(item))
                time.sleep(self.Gap_Insecond) #wait for few seconds.  
   
        except: 
            print('url "{}" went wrong'.format(url))  
            urls[url]='999'
                #not to consider failed pages. status_code 400s may need manual handling of they are high priority pages
        finally: 
            self.allurls=urls
            #csv_columns = ['url', 'status_code']  
            #try:
                #with open(self.existingurlfile, 'w', newline='', encoding='utf-8') as csvfile:
                    #writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    #writer.writeheader()
                    #for data in urls:
                        #writer.writerow(data)
            #except IOError:
                #print("I/O error")
    
            self.Save_Summaries() 
            
    """
        display summaries 
    """
    
    def Save_Summaries(self): 
        if self.allurls:
            df_urls=pd.DataFrame([[i, k] for i, k in (zip(self.allurls.keys(), self.allurls.values()) )], columns=['url', 'status_code'])
            
            #df_urls=pd.DataFrame(list(c.allurls.items()), columns=['url', 'status_code'])
            print('Summary for college ', self.college)
            print('\n')
            print(df_urls.groupby('status_code').count().reset_index())
            #save file as well  
            try:
                with open(self.existingurlfile, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(['url','status_code'])
                    for url, status_code in self.allurls.items():  
                        writer.writerow([url, status_code])  
            except IOError:
                print('IO Error in saving summaries')
                
        if self.files:
            print('\nThe following {} file(s) are generated. '.format(len(self.files)))
            pprint(self.files) 
    """
        read one page
    """
    def Read_Oneurl(self, url):  
        response=requests.get(url, self.headers)  
        if response.status_code==200: 
            return self.Get_Pagetext(response)
        else: 
            return [[None, None, None, None]]
    
    '''
        used for filter()
    '''
    def Tag_Visible(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True 

    '''
        get all text from page body
    '''
    def Get_Pagetext(self, response):
        soup = BeautifulSoup(response.text, 'html.parser') #conent
        texts = soup.findAll(text=True) 
        visible_texts = filter(self.Tag_Visible, texts)       

        return [ [t.parent.name,   
                 t.parent.previousSibling.name if t.parent.previousSibling!=None else None, 
                 t.nextSibling.name if t.nextSibling!=None else None,
                 re.sub(r'[\s+\t]',' ',t) ]  for t in visible_texts if len(t.strip())>2]

    '''
        save One Page
        filename, if not None, should not be full name. use import ntpath ntpath.basename("a/b/c")
    '''
    def SaveToCsv_FromUrl(self,url): # tab delimiter only  
        content=self.Read_Oneurl(url.strip()) #in format of (a,a,a,a)   
        self.SaveToCsv(url, content)
        #return SaveToCsv_FromResponse()
    
    '''
        save from resonse. called in the initial loop
    '''
    def SaveToCsv_FromResponse(self, url, response): 
        content= self.Get_Pagetext(response)
        self.SaveToCsv(url, content) 
        
    '''
        save to csv file and append it to self.files
    '''    
    def SaveToCsv(self, url, content):
        filename=url.replace('.', '_dot_').replace('/', '_').replace(':', '_')+'_'+datetime.now().strftime("%m_%d_%Y_%H_%M_%S")+'.csv'
        fullname=path.join(self.save_to_folder, filename)
        try:
            with open(fullname, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['url', 'parent', 'ps', 'ns', 'text'])
                for lll in content: 
                    lll.insert(0, url)
                    writer.writerow(lll)  
            self.files.append(fullname) 
        except IOError:
            print('failed to save file.')
            
    '''
        move file from one folder to another 
        pattern: regular expression
    '''
    def MoveFiles(self, destination_folder, source_folder=None,  pattern=None): 
        if source_folder==None:
            source_folder=self.save_to_folder
        if source_folder==None:
            print('Please specify folder with the files. ')
            return

        if destination_folder==None or path.isdir(destination_folder)==False:
            print('Need a valid destination folder')
            return
        if pattern==None or pattern=="":
            pattern="csv$" 
        files = [f for f in os.listdir(self.save_to_folder) if re.match(pattern, f) and path.isfile(path.join(source_folder, f))]
        for f in files:
            shutil.move(path.join(source_folder, f), destination_folder)  
    
    '''
        Merger files into one consolidated file by college. 
        drop duplicates 
        use relative folder path only. ./file name
    '''
    def Merge(self, merged_file_folder=None, isRemoveRawFile=False ): 
        if merged_file_folder==None:
            merged_file_folder='./training'  
        
        elif not re.match(r'^\.',merged_file_folder):
            print('Please provide a destination folder for merged file. e.g., ./training')
            return
        #safely create file
        Path(path.join(os.getcwd(), merged_file_folder)).mkdir(parents=True, exist_ok=True)
        
        merged_file=path.join(path.split(self.existingurlfile)[0], 'merged_'+path.split(self.existingurlfile)[1])
            
        #loaded files
        files=[(path.join(self.save_to_folder, f), 1)  for f in os.listdir(self.save_to_folder) if re.match(r'^http.+\d+.+csv$', f)]

        if path.exists(merged_file):
            files.append((merged_file, 0))

        df_combined = pd.concat([pd.read_csv(f[0] , sep='\t') for f in files])
        #drop dups
        df_combined.drop_duplicates(inplace=True)
        df_combined.to_csv( merged_file, index=False, sep='\t', encoding='utf-8')

        if isRemoveRawFile:
            for f in files:
                if f[1]==1:
                    os.remove(f[0])
'''
c=CollegeCrawl('stanford', 'https://www.stanford.edu')
ab=c.GetAllUrls()  #https://admission.stanford.edu/apply/
c.Merge(isRemoveRawFile=True)
'''