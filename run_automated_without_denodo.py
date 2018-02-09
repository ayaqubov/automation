######################## weekly running script
##### take the time of the two days before
import pandas as pd
import re
from langdetect import detect
from nltk.tokenize import sent_tokenize, word_tokenize
import nltk
from textblob import TextBlob, Word, Blobber
import textblob_fr
import textblob_de
import os
os.environ["NLS_LANG"] = ".AL32UTF8" 

import sys
reload(sys)
sys.setdefaultencoding('utf8')


# the code will work as follows:
# firstly read all the data by doing the connection, the connection is to the new place(but for now it is for the older place)
# firstly connect to the log data and read the latest 'ok' log 
# define new dates of data processing and follow
# then check the language and accordingly process the data(language checking is important)
# after processing and putting data into the dataframe use the sqlalchemy to write it into the database tables provided
# for that you need to be able to write into the databases(which is already ready)

# will save the reviews
#mycolumns=['REVIEW_WID','SKU','COUNTRY','SITE_URL','REVIEW_POSTED_DATE','STAR_RATING','REVIEW_FULL_TEXT']
mycolumns=['REVIEW_WID','SKU','COUNTRY','SITE_URL','REVIEW_POSTED_DATE','STAR_RATING','REVIEW_FULL_TEXT','PRODUCT_TYPE','PRODUCT_GROUP','PRODUCT_LINE_NAME']
mycolumnsdf=['REVIEW_WID','SKU','COUNTRY','SITE_URL','REVIEW_POSTED_DATE','STAR_RATING','REVIEW_FULL_TEXT','PRODUCT_TYPE','PRODUCT_GROUP','PRODUCT_LINE_NAME','LANGUAGE']
#mycolumns=['REVIEW_WID','SKU','PRODUCT_LINE_NAME','PRODUCT_TYPE','PRODUCT_GROUP','COUNTRY','SITE_URL','REVIEW_POSTED_DATE','STAR_RATING','REVIEW_FULL_TEXT']
df_aws_reviews=pd.DataFrame(columns=mycolumnsdf)


###################################   this part may be changed depending on the new database connection
# firstly read all the data by doing the connection, the connection is to the new place
# connection to database
import cx_Oracle
df_english=[]
con = cx_Oracle.connect('whread/whread@db0301-vip.logitech.com:1699/DWPROD')
#print con.version
cur = con.cursor()

#calculate the number of reviews in the database:
cur.execute('SELECT COUNT(*) AS FROM EDW_PRES.EXTERNAL_REVIEWS_DATA')
for row in cur:
    num_of_rows=row[0]
print("Num of rows in review table is "+ str(num_of_rows))
#######################################


#############################################
#firstly connect to the log data and read the latest 'ok' log 
####################################################
from datetime import datetime
java_path = "/usr/lib/jvm/jre-1.8.0"
os.environ['JAVAHOME'] = java_path
os.environ['JAVAHOME']
os.environ['CLASSPATH']='/home/ayaqubov/automation/jdbc_driver/denodo-vdp-jdbcdriver-6.0.0_1.jar:/home/ayaqubov/automation/jdbc_driver/ojdbc6.jar'
import jaydebeapi
# #os.environ['CLASSPATH']
# # be careful for last comma
# conn=jaydebeapi.connect("com.denodo.vdp.jdbc.Driver","jdbc:vdb://aws13dnddev02:9999/dbmetadata",["python_user","logitech123"],["/home/ayaqubov/automation/jdbc_driver/denodo-vdp-jdbcdriver-6.0.0_1.jar",])
# sql_query="""SELECT * FROM python_job_log ORDER BY id_job DESC LIMIT 1"""
# curs = conn.cursor()
# curs.execute(sql_query)
# row = curs.fetchone()
# last_id=row[0]
# last_status=row[2]
# lstart=row[3]
# lend=row[4]
# dlstart=datetime.strptime(lstart,'%Y-%m-%d %H:%M:%S')
# dlend=datetime.strptime(lend,'%Y-%m-%d %H:%M:%S')
# for now put all statuses to be equal to the 
#if last_status=='end':
	
#############################################

#########################################
#log data processing 
import mysql.connector
try:
    cnx=mysql.connector.connect(host='bi1.clk8xdvewu4b.us-west-2.rds.amazonaws.com',database='pentaho_audit',user='pentaho_audit',password='$Logitech123',port='3306')
    print('I connected')
except:
    print('Not connected')
job_cur=cnx.cursor()

job_cur.execute("""SELECT COUNT(*) FROM PYTHON_JOB_LOG""")
for row in job_cur:
    num_of_rows=row[0]
#print("Num of rows in review table is "+ str(num_of_rows))
last_job_log_checker="""SELECT * FROM PYTHON_JOB_LOG ORDER BY id_job DESC LIMIT 1"""
job_cur.execute(last_job_log_checker)
for row in job_cur:
    last_id=int(str(row[0]))
    last_status=row[2]
    lstart=row[3]
    lend=row[4]
dlstart=datetime.strptime(str(lstart),'%Y-%m-%d %H:%M:%S')
dlend=datetime.strptime(str(lend),'%Y-%m-%d %H:%M:%S')


#########################################

##############################################
#define new dates of data processing and follow
# then if the previous status was 'fail' , take the result and re-run again or ? # at this point we'll 'end'
# because we anyway do the 
if(last_status=='end'):
    #replaced_hour=dlend.hour+1
    #newstart=dlend.replace(hour=replaced_hour)
    replaced_minute=dlend.minute+1
    newstart=dlend.replace(minute=replaced_minute)
    newend=datetime.now().replace(second=0,microsecond=0)
else: # the case where the last status was 'fail'
    newstart=dlstart
    newend=datetime.now().replace(second=0,microsecond=0)

	
#newid=int(last_id)+1
newid=int(str(last_id))+1
newjobname='process_review'
newstatus='end'
newuser='ayaqubov'

snewstart=str(newstart)
msnewend=str(newend)
from datetime import timedelta
newenddate=datetime.strptime(msnewend,'%Y-%m-%d %H:%M:%S')
newenddate=newend.replace(hour=0,minute=0,second=0)
end_day=newenddate-timedelta(days=1)
#datetime.strptime(snewstart,'%Y-%m-%d %H:%M:%S')
snewend=str(end_day)

####################################################

#############################################
# making the query:
# get the data from AWS consisting of the columns specified and time specified where we only extract the reviews after that time
def make_quer_date(mycolumns,inityear,initmonth,initday,posted_or_extracted,endyear,endmonth,endday):
    # specify last parameter as 0 or 1 and depending on that parameter
    # posted_or_extracted---->1 for extraction
    # posted----->2
    # data will be extracted 
    # check the time of the row  
    quer="SELECT \n"
    for i in range(0,len(mycolumns)):
        if(i!=len(mycolumns)-1):
            quer=quer+mycolumns[i]+','
        else:
            quer=quer+mycolumns[i]
    if(len(str(initday))==1):
        str_day="0"+str(initday)
    else:
        str_day=str(initday)
    if(len(str(initmonth))==1):
        str_month="0"+str(initmonth)
    else:
        str_month=str(initmonth)
    #quer+="\"
    
    if(len(str(endday))==1):
        estr_day="0"+str(endday)
    else:
        estr_day=str(endday)
    if(len(str(endmonth))==1):
        estr_month="0"+str(endmonth)
    else:
        estr_month=str(endmonth)
    
    quer+=" \n"
    #quer=quer+' FROM EDW_PRES.EXTERNAL_REVIEWS_DATA where rownum<='+rows
    quer+="FROM EDW_PRES.EXTERNAL_REVIEWS_DATA where "
    quer+="\n"
    if(posted_or_extracted==1):
        d=" EXTRACTION_DATE"
    elif(posted_or_extracted==2):
        d=" REVIEW_POSTED_DATE"
    else:
        #error('The last parameter must be either e or p')
        raise ValueError("The time parameter must be either 1 or 2")
    quer+=d
    quer+=" >= to_timestamp("+"'"+str_day+'-'+str_month+'-'+str(inityear)+' 00:00:00'+"'"
    quer+=", "
    quer+="'"+"dd-mm-yyyy hh24:mi:ss"
    quer+="\'"
    quer+=")"
    #upper
    quer+=" and "
    quer+=d
    quer+=" <= to_timestamp("+"'"+estr_day+'-'+estr_month+'-'+str(endyear)+' 00:00:00'+"'"
    quer+=", "
    quer+="'"+"dd-mm-yyyy hh24:mi:ss"
    quer+="\'"
    quer+=")"
    return quer

myque=make_quer_date(mycolumns,newstart.year,newstart.month,newstart.day,2,end_day.year,end_day.month,end_day.day)
cur.execute(myque)

# until this part we will be able to get the query to run

##############################################

###########################################################
# here comes the check the language part and write correspondingly to the df_aws_reviews dataframe

from nltk.tokenize import sent_tokenize, word_tokenize
## data processing
print('Processing...')
index=0
i_debug=0
row = cur.fetchone()
while row:
    #print i_debug
    #if i_debug==49:
    #    i_debug+=1
    #    continue;
#     if i_debug==49:
#         print 'Yoo'
    row_l=list(row)
    #print(row_l)
    review_text=row_l[6]# type is lob of oracle
    #print type(str(review_text))
    #if(isinstance(review_text,basestring)):
    review_str=str(review_text)
    ureview_str=unicode(review_str,"utf-8")
    #print(review_str)
    #print(len(review_str))
    contain_l=re.search('[a-zA-Z]', review_str)
    if(contain_l!='None'):
        # handle japanese,arabic,chinese cases because they appear as the ?? marks in the results
        try:
            text_lang=detect(ureview_str)
            #text_lang2=identify_lang(review_str)
        except:
            print 'Error in reading, keep reading'
            i_debug+=1
            row=cur.fetchone()
            continue
        #check_words=word_tokenize(review_str)
        #for iiii in range(0,len(check_the_words)):
        #    if (check_the_words[iiii]=='product' or check_the_words[iiii]=='excellent'  or check_the_words[iiii]=='mouse'):
        #        break
        #            #continue
        if(text_lang=='en'):
            one_review=row_l
            one_review[6]=ureview_str
            one_review.append('English')
            #one_review=pd.Series(mylist);
            #df_aws_review.append(one_review,ignore_index=True)
            df_aws_reviews.loc[index]=one_review;
            #print(df_aws_reviews.shape)
            index+=1;
        #if (text_lang!='fr' or review_str.contains('product')!=-1 or review_str.contains('excellent')!=-1 or review_str.contains('mouse')!=-1):
        #    continue
        #    #do nothing
        
        #if(text_lang=='en'):
        elif (text_lang=='fr'):
            if('product' in ureview_str or 'excellent' in ureview_str or 'mouse' in ureview_str):
                #process as an english review:
                one_review=row_l
                one_review[6]=ureview_str
                one_review.append('English')
                #one_review=pd.Series(mylist);
                #df_aws_review.append(one_review,ignore_index=True)
                df_aws_reviews.loc[index]=one_review;
                #print(df_aws_reviews.shape)
                index+=1;
            else:
                one_review=row_l
                one_review[6]=ureview_str
                one_review.append('French')
                #one_review=pd.Series(mylist);
                #df_aws_review.append(one_review,ignore_index=True)
                df_aws_reviews.loc[index]=one_review;
                #print(df_aws_reviews.shape)
                index+=1;

        elif(text_lang=='de'):
            one_review=row_l
            one_review[6]=ureview_str
            one_review.append('German')
            #one_review=pd.Series(mylist);
            #df_aws_review.append(one_review,ignore_index=True)
            df_aws_reviews.loc[index]=one_review;
            #print(df_aws_reviews.shape)
            index+=1;
                # check type is string:isinstance(s, basestring)
    i_debug+=1
    row=cur.fetchone()
####n in above i added the part where i can get the language information later
#### it means that i need to find 


########################################################

#########################################################
#functions to process the data:
def count_words(sentence):
    words=word_tokenize(sentence)
    l=len(words)
    return l

def get_sentiment_en(sentence):
    from textblob import TextBlob
    # this function is the simplest function
    blob=TextBlob(sentence)
    sentiment_pol=blob.sentiment.polarity
    #sentiment_sub=TextBlob(sentence).sentiment.subjectivity
    #sentiment_=sentiment_pol
    return sentiment_pol
from textblob_fr import PatternTagger, PatternAnalyzer
from textblob import Blobber
def get_sentiment_fr(sentence):
    mysentence=str(sentence)
    tb = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
    sentim = tb(mysentence).sentiment
    sentiment_pol=sentim[0]
    sentiment_sub=sentim[1]
    return sentiment_pol

from textblob_de import TextBlobDE as TextBlob
from textblob_de import PatternTagger
def get_sentiment_de(sentence):
    blob = TextBlob(sentence)
    sentim=blob.sentiment
    sentiment_pol=sentim[0]
    sentiment_sub=sentim[1]
    return sentiment_pol
import langid
def identify_lang(sentence):
    cl=langid.classify(sentence)
    lan=cl[0]
    return lan
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
ps = PorterStemmer()
def stem_word(in_word):
    stemmed_word=ps.stem(in_word)
    return stemmed_word

#########################################################

#########################################################
# division into the sentences
df_num_of_rows=df_aws_reviews.shape[0]
#mycolumns=['REVIEWSENTENCE_WID','REVIEW_WID','SENTENCE_ID','SKU','COUNTRY','SITE_URL','REVIEW_POSTED_DATE','WORD_COUNT',
#                    'SENTIMENT','STAR_RATING','SENTENCE','PRODUCT_TYPE','PRODUCT_GROUP','PRODUCT_LINE_NAME']
mycolumns_sentence=['reviewsentence_wid','review_wid','sentence_id','sku','country','site_url','review_posted_date','word_count',
                    'sentiment','star_rating','sentence','product_type','product_group','product_line_name','language']
df_aws_sentences=pd.DataFrame(columns=mycolumns_sentence)

# adding sentences
index_=0
for i in range(0,df_num_of_rows):
    this_review=df_aws_reviews.iloc[i,6]
    # use try except because errpr was occuring in some cases
    try:
        sentences_this_review=sent_tokenize(this_review)
    except:
        print("Error in sentence tokenizing")
        continue
    num_of_sents=len(sentences_this_review)
    current_review_id=str(int(df_aws_reviews.iloc[i,0]))
    #print(current_review_id)
    if(num_of_sents!=0):
        sent_id=0
        for j in range(0,num_of_sents):
            current_sentence=sentences_this_review[j]
            if(current_sentence in ["!","?","."]):
                continue
            word_count=count_words(current_sentence)
            reviewsentence_id=int(current_review_id+str(sent_id))#int(current_review_id+'_'+str(sent_id))
            # Now calculate the polarity of sentece:
            #sentiment_=get_sentiment(current_sentence)
            if(df_aws_reviews.iloc[i,10]=='English'):
                sentiment_=get_sentiment_en(current_sentence)
            elif(df_aws_reviews.iloc[i,10]=='French'):
                sentiment_=get_sentiment_fr(current_sentence)
            elif(df_aws_reviews.iloc[i,10]=='German'):
                sentiment_=get_sentiment_de(current_sentence)

            one_row=[reviewsentence_id,current_review_id,sent_id,df_aws_reviews.iloc[i,1],df_aws_reviews.iloc[i,2],df_aws_reviews.iloc[i,3],df_aws_reviews.iloc[i,4],word_count,sentiment_,df_aws_reviews.iloc[i,5],current_sentence,df_aws_reviews.iloc[i,7],df_aws_reviews.iloc[i,8],df_aws_reviews.iloc[i,9],df_aws_reviews.iloc[i,10]]
            df_aws_sentences.loc[index_]=one_row
            sent_id+=1
            index_+=1

df_aws_sentences[['reviewsentence_wid','review_wid','sentence_id','word_count','star_rating']]=df_aws_sentences[['reviewsentence_wid','review_wid','sentence_id','word_count','star_rating']].astype(int)		
cols_word_freq= ['reviewsentence_wid','review_wid','sentence_id','word','translated_word','freq']
df_sents_num_of_rows=df_aws_sentences.shape[0]
########################################################


##########################################################
# word frequency table related part
df_word_freq=pd.DataFrame(columns=cols_word_freq)
# get rid of commas etc
from nltk.tokenize import RegexpTokenizer
tokenizer = RegexpTokenizer(r'\w+')

import re
def check_num(input_s): 
    num_format = re.compile("^[\-]?[1-9][0-9]*\.?[0-9]+$")
    isnumber = re.match(num_format,input_s)
    #isnumber=~
    if isnumber:
        return True
    else:
        return False
def check_letter(input_s):
    #remove len 1 and 2s(come back for len 2 later)
    l=len(input_s)
    if(l==1 or l==2):
        return True
    return False

from nltk.corpus import stopwords
#stopwords_ = set(stopwords.words('english'))
stopwords_fr=set(stopwords.words('french'))
stopwords_en=set(stopwords.words('english'))
stopwords_ge=set(stopwords.words('german'))


windex_=0

for i in range(0,df_sents_num_of_rows):
    #print(i)
    sentence=df_aws_sentences.iloc[i,10]
    #words=word_tokenize(sentence)
    # maybe use try-except block as follows:
    #try:
    #words=tokenizer.tokenize(sentence)
    #except:
    #print("error in word tokenizing")

    words=tokenizer.tokenize(sentence)
    tags_=nltk.pos_tag(words)
    num_words=len(words)
    for j in range(0,num_words):
        word=words[j]
        wordlow=word.lower()
        # check if it is noun here
        translated=wordlow  ## for another language we need translation
        freq=1
        w_isnum=check_num(wordlow)
        one_two_let=check_letter(wordlow)
        if(df_aws_sentences.iloc[i,14]=='English'):
            stopwords_=stopwords_en
        if(df_aws_sentences.iloc[i,14]=='French'):
            stopwords_=stopwords_fr
        if(df_aws_sentences.iloc[i,14]=='German'):
            stopwords_=stopwords_ge
        if(wordlow in stopwords_ or w_isnum or one_two_let):
            continue
            
        
        if(tags_[j][1]=='NN' or tags_[j][1]=='NNS' or tags_[j][1]=='NNP'):
            # since we expect aspects are more likely to be among the nouns
            
            one_row=[df_aws_sentences.iloc[i,0],df_aws_sentences.iloc[i,1],df_aws_sentences.iloc[i,2],wordlow,translated,freq]
            df_word_freq.loc[windex_]=one_row
            windex_+=1
        #print(windex_)

df_word_freq[['reviewsentence_wid','review_wid','sentence_id','freq']]=df_word_freq[['reviewsentence_wid','review_wid','sentence_id','freq']].astype(int)

###############################################################



#########################################################
# this part will the results into the 2 tables
# after processing and putting data into the dataframes use the sqlalchemy to write it into the database tables provided

from sqlalchemy import create_engine
engine = create_engine('postgresql://logi_mobile:logi_mobile@aws13circleanalytics.clk8xdvewu4b.us-west-2.rds.amazonaws.com:5432/postgres')
################
#engine = create_engine('postgresql://python_user:logitech123@aws13dnddev02:9999/edw_redshift_dev')
#df_aws_sentences.to_sql('sa_review_sentences', engine,index=False,if_exists='append')
#df_word_freq.to_sql('sa_word_freqs',engine,index=False,if_exists='append')
df_aws_sentences.to_sql('sa_review_sentences', engine,schema='Logi_Vc_Logs' ,index=False,if_exists='append')
df_word_freq.to_sql('sa_word_freqs',engine,schema='Logi_Vc_Logs',index=False,if_exists='append')
################
#df_aws_sentences.to_sql('sa_review_sentences_12')

##### if you weant to delete the contents of the table use the followng command:
### TRUNCATE table

############################################################


###########################################################
##


newdata=(newid,newjobname,newstatus,snewstart,snewend,newuser)

insert_stmt = (
  """INSERT INTO PYTHON_JOB_LOG (id_job, jobname, status, startdate,enddate,executing_user)"""
  "VALUES (%s, %s, %s, %s,%s,%s)"
)

#insertion="""INSERT INTO PYTHON_JOB_LOG (id_job, jobname, status, startdate,enddate,executing_user) VALUES (19,'Arzi','end','18-06-15 10:34:09','20-06-16 10:34:09','ara')"""

#insertion2="""INSERT INTO PYTHON_JOB_LOG VALUES (34,'process_review', 'end', '2017-08-04 01:00:00', '2017-08-11 02:21:00', 'ayaqubov')"""

#data = (1234, 'Araz_test', 'end', datetime.date(2012, 3, 23), datetime.date(2015, 3, 23),'ya')
#data = (3342, 'Araztest', 'end','18-06-14 10:34:09' ,'20-09-17 10:34:09','ya')
### insert_stmt % newdata also works for elegancy, just need to handle the errors
sql_query= "INSERT INTO PYTHON_JOB_LOG VALUES (" + str(newid) + """,'"""+newjobname+"""'"""+""",'"""+newstatus+"""'"""+""",'"""+snewstart+"""'"""+""",'"""+snewend+"""'"""+""",'"""+newuser+"""'"""+")"


try:
    job_cur.execute(sql_query)
    #job_cur.execute(insertion2)
    print("successful insertion to log table")
except:
    print("could not connect")
cnx.commit()
