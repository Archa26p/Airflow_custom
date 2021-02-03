# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 14:48:09 2020

@author: gmanthri
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Oct  1 12:25:37 2020

@author: gmanthri

1.Login to Cloud
2.Create temp folder
3.Create reports
4.Run reports
5.Generate csv files
6.Process csv files to generate final Audit file
7.Upload to DB
8.Delete temp folder
9.Logout
"""

import requests
import re
import base64
from lxml import etree as ET
from datetime import datetime
from xmlutils.xml2csv import xml2csv
import pandas as pd
import json
import sqlite3
from collections import defaultdict
import os
import time
import logging
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator

var_name = "PROD"
dag_prefix = ""
cloud_credential = Variable.get(var_name + "_credential", deserialize_json=True)
cloud_conf = Variable.get(var_name + "_AUDIT_conf", deserialize_json=True)

# print log to Audit.log
args = {
    "package_root": cloud_conf['package_root'],
    'db_path': cloud_conf['db_path'],
    'folderAbsolutePath': cloud_conf['audit_folder_abs_path'],
    'tablename': cloud_conf['tablename'],
    'refresh': cloud_conf['refresh'],
    'fusionDB': cloud_conf['fusionDB'],

}
print(args)
wsurl=''
audit_uid=''
audit_pwd=''

def log(message):
    print('iniside')
    print(message)
    # logging.debug(message)


# login to cloud and fetch the sessiontoken to use in other methods
def login(url, username, password, templatesPath):
    log('Start : login')
    header = {"SOAPAction": "login",
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'Login_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    soap_text = body.format(USERNAME=username, PASSWORD=password)

    response = requests.post(url=url, data=soap_text, headers=header)
    if int(response.status_code) == 200:
        log('login Success')
        token = re.findall("<loginReturn>(.*?)</loginReturn>", response.content.decode('utf-8', "ignore"))
        return token[0]
    else:
        log('Login exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
        return ''


# logout of the session at the end
def logout(url, bipSessionToken, templatesPath):
    log('Start : logout')
    header = {"SOAPAction": "logout",
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'Logout_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    soap_text = body.format(BIPSESSIONTOKEN=bipSessionToken)

    response = requests.post(url=url, data=soap_text, headers=header)
    if int(response.status_code) == 200:
        log('logout Success')
        logoutReturn = re.findall("<logoutReturn>(.*?)</logoutReturn>", response.content.decode('utf-8', "ignore"))
        return logoutReturn[0]
    else:
        log('Logout exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
        return ''


# check if temp folder already exists
def isFolderExist(url, bipSessionToken, folderAbsolutePath, templatesPath):
    log('Start : isFolderExist')
    header = {"SOAPAction": "isFolderExistInSession",
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'isFolderExistInSession_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    soap_text = body.format(BIPSESSIONTOKEN=bipSessionToken, FOLDERABSOLUTEPATH=folderAbsolutePath)
    response = requests.post(url=url, data=soap_text, headers=header)
    if int(response.status_code) == 200:
        log('isFolderExist Success')
        isFolderExistReturn = re.findall("<isFolderExistInSessionReturn>(.*?)</isFolderExistInSessionReturn>",
                                         response.content.decode('utf-8', "ignore"))
        return isFolderExistReturn[0]
    else:
        log('isFolderExist exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
        return ''


# create temp folder
def createReportFolder(url, bipSessionToken, folderAbsolutePath, templatesPath):
    log('Start : createReportFolder')
    header = {"SOAPAction": "createReportFolderInSession",
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'createReportFolderInSession_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    soap_text = body.format(BIPSESSIONTOKEN=bipSessionToken, FOLDERABSOLUTEPATH=folderAbsolutePath)
    response = requests.post(url=url, data=soap_text, headers=header)
    if int(response.status_code) == 200:
        log('createReportFolder Success')
        createReportFolder = re.findall("<createReportFolderInSessionReturn>(.*?)</createReportFolderInSessionReturn>",
                                        response.content.decode('utf-8', "ignore"))
        return createReportFolder[0]
    else:
        log('createReportFolder exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
        return ''


# delete folder after fetching the data
def deleteFolder(url, bipSessionToken, folderAbsolutePath, templatesPath):
    log('Start : deleteFolder')
    header = {"SOAPAction": "deleteFolderInSession",
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'deleteFolderInSession_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    soap_text = body.format(BIPSESSIONTOKEN=bipSessionToken, FOLDERABSOLUTEPATH=folderAbsolutePath)
    response = requests.post(url=url, data=soap_text, headers=header)
    if int(response.status_code) == 200:
        log('deleteFolder Success')
        deleteFolderReturn = re.findall("<deleteFolderInSessionReturn>(.*?)</deleteFolderInSessionReturn>",
                                        response.content.decode('utf-8', "ignore"))
        return deleteFolderReturn[0]
    else:
        log('deleteFolder exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
        return ''


# fetch the column names from sql to be passed while creating the report
def readAliases(dmSQL):
    log('Start : readAliases')
    string = dmSQL.upper().replace('\n', ';')
    column_string = re.findall("SELECT (.*?) FROM ", string)
    if column_string:
        column_string = re.split(r',;', column_string[0])
        column_string = [i.strip() for i in column_string]
        a = [x.split(' ') for x in column_string if x != '']
        return {x[0].replace(';', ''): (x[1].replace(';', '') if len(x) > 1 else x[0].replace(';', '')) for x in a}
    return


# creating the xdm file to passed while creating the data model
def createXDM(dmName, dmDesc, dmSQL, fusionDB, templatesPath):
    log('Start : createXDM')
    soap_template = os.path.join(templatesPath, 'DataModel_Template.xml')
    tree = ET.parse(soap_template)
    root = tree.getroot()
    nsmap = root.nsmap
    group_tag = tree.find(".//group", nsmap)
    # appending element tag for each column name
    columnAliases = readAliases(dmSQL)
    for columnName in columnAliases.keys():
        ET.SubElement(group_tag, "element", name=columnAliases[columnName], value=columnAliases[columnName],
                      label=columnAliases[columnName])
    body = ET.tostring(tree).decode('utf-8')
    body = body.format(DM_NAME=dmName, DM_DESCRIPTION='<![CDATA[' + dmDesc + ']]>', DM_SQL='<![CDATA[' + dmSQL + ']]>',
                       DATABASE=fusionDB)
    return body


# create data model in cloud with xdm
def createDataModel(url, bipSessionToken, objectName, objectData, folder, objectDesc, templatesPath):
    log('Start : createDataModel for {} at {}'.format(objectName, datetime.now()))
    headers = {'content-type': 'text/xml', "SOAPAction": "createObjectInSession"}
    soap_template = os.path.join(templatesPath, 'createObjectInSession_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    body = body.format(OBJECT_NAME=objectName, OBJECT_TYPE='xdm', OBJECT_DESCRIPTION=objectDesc, OBJECT_DATA=objectData,
                       REPORT_ABSOLUTE_PATH=folder, BIPSESSIONTOKEN=bipSessionToken)
    response = requests.post(url, data=body, headers=headers)
    if int(response.status_code) != 200:
        log('createDataModel exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))
    else:
        log('{} data model created'.format(objectName))

    log('End : createDataModel for {} at {}'.format(objectName, datetime.now()))


# run the data model to fetch the output in xml format
def runDataModel(url, bipSessionToken, objectName, folder, templatesPath):
    log('Start : runDataModel for {} at {}'.format(objectName, datetime.now()))
    abspath = folder + '/' + objectName + '.xdm'
    log(abspath)
    headers = {"SOAPAction": "runDataModelInSession",
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    soap_template = os.path.join(templatesPath, 'runDataModelInSession_Template.xml')
    tree = ET.parse(soap_template)
    body = ET.tostring(tree).decode('utf-8')
    body = body.format(REPORT_ABSOLUTE_PATH=abspath, BIPSESSIONTOKEN=bipSessionToken)
    response = requests.post(url, data=body, headers=headers)

    if int(response.status_code) == 200:
        log('Extract Success')
        xml_data = re.findall("<reportBytes>(.*?)</reportBytes>", response.content.decode('utf-8', "ignore"))
        string = xml_data[0]
        b = base64.b64decode(string)
        sample_string = b.decode("utf-8", "ignore")

        response_xml_file = os.path.join(args['package_root'], objectName + '.xml')
        with open(response_xml_file, 'w', encoding="utf-8") as response_file:  #
            response_file.write(sample_string)
    else:
        log('runDataModel exception. Status : {}'.format(response.status_code))
        log('Response content : {}'.format(response.content))

    log('End : runDataModel for {} at {}'.format(objectName, datetime.now()))


# creating the audit report
def createAuditReport(url, bipSessionToken, folderAbsolutePath, extracturl, templatesPath, sqlpath, planname, maxdate,
                      meassurename, fusionDB):
    log('Start : createAuditReport')
    now = datetime.now()
    fileName = 'AuditReport'
    fileName = 'AuditReport_' + str(now.strftime("%d%m%Y%H%M%S"))
    log(fileName)
    auditsqlfile = os.path.join(sqlpath, 'AuditSql.txt')
    with open(auditsqlfile) as f_obj:
        qstring = f_obj.read()
    print("planname is ",planname)
    if planname:
        print('inside plan name')
        qstring += " AND NVL(p.compile_designator,'All') in ('All',{PLANNAME}) ".format(
            PLANNAME=planname)

    if maxdate:
        qstring += " AND TO_TIMESTAMP(to_char(a.creation_date,'YYYY-MM-DD HH24:MI:SSxFF'),'YYYY-MM-DD HH24:MI:SSxFF') >= TO_TIMESTAMP('{MAXDATE}','YYYY-MM-DD HH24:MI:SSxFF') ".format(
            MAXDATE=maxdate)

    if meassurename:
        qstring += " AND m.name in ({MEASURE}) ".format(MEASURE=meassurename)
    print("qstring",qstring)
    qstring = qstring.replace('[', '').replace(']', '')
    xdm = createXDM(fileName, 'temp', qstring, fusionDB, templatesPath)
    objectData = base64.b64encode(bytes(xdm, encoding='utf-8'))
    objectData = objectData.decode('utf-8')
    createDataModel(url, bipSessionToken, fileName, objectData, folderAbsolutePath, 'temp', templatesPath)
    time.sleep(5)
    runDataModel(extracturl, bipSessionToken, fileName, folderAbsolutePath, templatesPath)
    return os.path.join(args['package_root'], fileName)


# creating the plan members report
def createMemberReport(url, bipSessionToken, folderAbsolutePath, extracturl, templatesPath, sqlpath, fusionDB):
    log('Start : createMemberReport')
    now = datetime.now()
    fileName = 'PlanMembers'
    fileName = 'PlanMembers_' + str(now.strftime("%d%m%Y%H%M%S"))
    log(fileName)
    membersqlfile = os.path.join(sqlpath, 'MemberSql.txt')
    print(membersqlfile)
    with open(membersqlfile) as f_obj:
        qstring = f_obj.read()

    xdm = createXDM(fileName, 'temp', qstring, fusionDB, templatesPath)
    objectData = base64.b64encode(bytes(xdm, encoding='utf-8'))
    objectData = objectData.decode('utf-8')
    createDataModel(url, bipSessionToken, fileName, objectData, folderAbsolutePath, 'temp', templatesPath)
    time.sleep(5)
    runDataModel(extracturl, bipSessionToken, fileName, folderAbsolutePath, templatesPath)
    return os.path.join(args['package_root'], fileName)


# converting xml to csv
def xmltocsv(filename):
    log('Start : xmltocsv')
    log('filename :' + filename)
    converter = xml2csv(filename + ".xml", filename + ".csv", encoding="utf-8")
    converter.convert(tag="G_1")


# combining all 8 details columns into 1 before processing the audit extract
def auditclean(filename):
    log('Start : auditclean')
    dfa = pd.read_csv(filename + '.csv')
    dfa = dfa.fillna('')
    collist = ['DETAILS1', 'DETAILS2', 'DETAILS3', 'DETAILS4', 'DETAILS5', 'DETAILS6', 'DETAILS7', 'DETAILS8']
    dfa['DETAILS'] = ''
    for i in collist:
        if i in dfa.columns:
            dfa['DETAILS'] += dfa[i].replace('#', '')
            dfa.drop([i], axis=1, inplace=True)
    dfa.to_csv(filename + '.csv', index=False)
    log('Clean Success')


# check for any new columns to be created in the DB table and create if any
def managecolumns(df, tablename, conn):
    cols = list(df.columns)
    print(cols)
    for col in cols:
        cur = conn.cursor()
        try:
            cur.execute('ALTER TABLE {} ADD COLUMN "{}" TEXT'.format(tablename, col))
            print('Created column {}'.format(col))
        except:
            # print('column exists')
            pass  # handle the error
        # print(f"SELECT COUNT(*) FROM pragma_table_info('{tablename}') WHERE name='{col}'")
        # print(conn,cur)
        # cur.execute(f"SELECT COUNT(*) FROM pragma_table_info('{tablename}') WHERE name='{col}'")
        # print('executed')
        # count=cur.fetchone()
        # print(count)
        # if count and count[0] == 0:
        #    log(col)
        #    cur.execute('ALTER TABLE {} ADD COLUMN "{}" TEXT'.format(tablename, col))
    print('done')


# remove any duplicate audits from table
def cleantable(tablename, conn):
    log('Clean started : {}'.format(datetime.now()))
    cur = conn.cursor()
    delsql = """DELETE from {TABLE} as a
where audit_id in (select audit_id from {TABLE}  as b
where b.audit_id=a.audit_id
and b.rec_creation_date>a.rec_creation_date)"""
    cur.execute(delsql.format(TABLE=tablename))
    conn.commit()
    log('Clean Completed : {}'.format(datetime.now()))


# check if any duplicate audits exist in table
def checkduplicate(conn, tablename):
    cur = conn.cursor()
    cur.execute("select count(*) from {} group by audit_id having count(*)>1".format(tablename))
    if cur.fetchone():
        return 'Yes'
    else:
        return 'No'


# fetch latest audits from the table
def fetchaudits(conn, tablename, datestr):
    cur = conn.cursor()
    cur.execute(
        "SELECT audit_id from {} where strftime('%Y-%m-%d %H:%M:%f',creation_date)='{}'".format(tablename, datestr))
    auditids = cur.fetchall()
    if auditids:
        return auditids
    else:
        return ''


# process the audit extract to derive actuals and insert into DB
def processaudit(memberfile, auditfile, conn, refresh, tablename, maxdate):
    log('Start : processaudit at {}'.format(datetime.now()))
    dfa = pd.read_csv(auditfile + '.csv')

    dfa.rename(columns=str.lower, inplace=True)  # GJ 9/10 - standardize to lower case col names
    for index, row in dfa.iterrows():
        x = json.loads(dfa.iloc[index]['details'])

        cl = x.get('combination')
        for c in cl:
            dfa.at[index, ('key' + str(c['levelId']))] = str(c.get('levelId')) + ":" + str(c.get('memberId'))

        fl = x.get('filters')
        tmp = defaultdict(list)

        for item in fl or []:
            tmp[item['levelId']].extend(item['memberIds'])

        fl = [{'levelId': k, 'memberIds': v} for k, v in tmp.items()]
        for f in fl or []:  # GM 9/24 - added or condition TypeError: ‘NoneType’ object is not iterable
            z = []
            for m in f.get('memberIds'):
                z.append(str(f.get('levelId')) + ":" + str(m))

            dfa.at[index, ('filter' + str(f['levelId']))] = str(z)

    ####################### Map Keys ###########################
    # Get Member Data. Build keys.
    dfm = pd.read_csv(memberfile + '.csv')
    dfm.rename(columns=str.lower, inplace=True)
    dfm = dfm[dfm['level_member_id'] != -1]  # -1 is for value not found
    dfm['level_member'] = dfm['level_id'].apply(str).str.cat(dfm['level_member_id'].apply(str), sep=":")
    dfm.set_index('level_member', inplace=True)

    # Dict for Level Id and names
    lvl_dict = dict(zip(dfm['level_id'].apply(str), dfm['level_name']))

    # For key columns in Audit DF, find Member values and put them in a new column
    col_lst = [x for x in dfa.columns if x.startswith('key')]

    # Replace member_id in DFA w Member name from DFM for all columns
    for y in col_lst:
        dfa[y.replace('key', '')] = dfa[y].map(dfm[dfm['level_id'] == int(y.replace('key', ''))].level_member_name)

    dfa.rename(columns=lvl_dict, inplace=True)
    dfa.columns = dfa.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    ####################### Map Filters ###########################
    # For key columns in Audit DF, find Member values and put them in a new column
    col_lst = [x for x in dfa.columns if x.startswith('filter')]

    dfb = dfa.copy()

    def except_keyerror(x):
        try:
            return (dfm.at[x, 'level_member_name'])
        except KeyError:
            return ''

    # Replace member_id in DFA w Member name from DFM for all columns
    for y in col_lst:
        dfb[y] = dfb[y].str.replace("\['", '').str.replace("']", '').str.replace("'", '').str.replace(" ", '')

        for index, row in dfb[~dfb[y].isnull()].iterrows():
            st = dfb.at[index, y]
            lst = st.split(',')
            res = list(map(lambda x: except_keyerror(x), lst))
            dfb.at[index, y.replace('filter', '')] = str(res)

            # Dict for Level Id and names used for Tab Filters
    lvl_dict2 = {k: 'TabFilter-' + v for (k, v) in lvl_dict.items()}

    dfb.rename(columns=(lvl_dict2), inplace=True)
    dfb['rec_creation_date'] = datetime.now()
    dfb.rename(columns=str.lower, inplace=True)  # GJ 9/10 - standardize to lower case col names
    dfb.columns = dfb.columns.str.replace(' ', '')
    print(refresh)
    if refresh == 'Yes':
        ifexists = 'replace'
    else:
        dupaudits = fetchaudits(conn, tablename, maxdate)
        if dupaudits:
            for dupaudit in dupaudits:
                i = dfb[(dfb.audit_id == dupaudit[0])].index
                dfb.drop(i, inplace=True)
        managecolumns(dfb, tablename, conn)
        ifexists = 'append'
    print('processing')

    # Write to File
    dfb.to_csv(os.path.join(args['package_root'], 'AuditResult.csv'))
    print('writ')

    dfb.to_sql(tablename, conn, if_exists=ifexists)
    dup_count = checkduplicate(conn, tablename)
    log(dup_count)
    if dup_count == 'Yes':
        cleantable(tablename, conn)

    log('End : processaudit at {}'.format(datetime.now()))


# fetch max date from table to be used to fetch incremental audits
def getmaxdate(conn, tablename):
    cur = conn.cursor()
    cur.execute("SELECT strftime('%Y-%m-%d %H:%M:%f',max(creation_date)) FROM {}".format(tablename))
    # data=cur.fetchall()
    # print(data)
    return cur.fetchone()[0]


# check if audit extract is empty
def except_EmptyDataError(auditfile):
    try:
        pd.read_csv(auditfile + '.csv')
        return False
    except pd.errors.EmptyDataError:
        log('No Audits Found')
        return True


# delete xml and csv files after processing
def deletefiles(filename):
    if os.path.exists(filename + ".xml"):
        os.remove(filename + ".xml")
    else:
        log("The file does not exist")
    if os.path.exists(filename + ".csv"):
        os.remove(filename + ".csv")
    else:
        log("The file does not exist")

    # refresh log file after it crosses 1MB


def managelog():
    if os.path.exists(os.path.join(args['package_root'], "Audit.log")):
        filesize = os.path.getsize(os.path.join(args['package_root'], "Audit.log"))
        if filesize > 1000000:
            os.remove("Audit.log")


def main():
    with open(os.path.join(args['package_root'], 'data.json')) as f:
        config_data = json.loads(f.read())

    wsurl = config_data['url']
    audit_uid = config_data['userName']
    audit_pwd = config_data['password']
    plannames = ','.join(["'" + p + "'" for p in config_data['plannames']])
    meassurenames = ','.join(["'" + m + "'" for m in config_data['measurenames']])
    print(plannames)
    print(meassurenames)

    managelog()
    logging.basicConfig(filename=os.path.join(args['package_root'], 'Audit.log'), level=logging.DEBUG)
    log(
        "===================================================={}====================================================".format(
            datetime.now()))
    startTime = datetime.now()
    

    username = audit_uid
    password = audit_pwd
    folderAbsolutePath = args['folderAbsolutePath']
    baseurl = wsurl
    planname = plannames

    meassurename = meassurenames
    connection = args['db_path']
    refresh = args['refresh']
    tablename = args['tablename']
    fusionDB = args['fusionDB']

    url = baseurl + "/xmlpserver/services/PublicReportService"
    objurl = baseurl + "/xmlpserver/services/v2/CatalogService"
    extracturl = baseurl + "/xmlpserver/services/v2/ReportService"
    templatesPath = os.path.join(args['package_root'], 'Templates')
    sqlpath = os.path.join(args['package_root'], 'Sql')
    # try:
    bipSessionToken = login(url, username, password, templatesPath)
    if bipSessionToken:
        conn = sqlite3.connect(connection)
        if refresh != 'Yes':
            maxdate = getmaxdate(conn, tablename)
        else:
            maxdate = ''
        isFolderExistReturn = isFolderExist(url, bipSessionToken, folderAbsolutePath, templatesPath)
        if isFolderExistReturn == 'false':
            createReportFolderReturn = createReportFolder(url, bipSessionToken, folderAbsolutePath, templatesPath)
        auditfile = createAuditReport(objurl, bipSessionToken, folderAbsolutePath, extracturl, templatesPath,
                                      sqlpath, planname, maxdate, meassurename, fusionDB)
        memberfile = createMemberReport(objurl, bipSessionToken, folderAbsolutePath, extracturl, templatesPath,
                                        sqlpath, fusionDB)
        xmltocsv(auditfile)
        xmltocsv(memberfile)
        if not except_EmptyDataError(auditfile):
            auditclean(auditfile)
            processaudit(memberfile, auditfile, conn, refresh, tablename, maxdate)
        conn.close()
        deleteFolderReturn = deleteFolder(url, bipSessionToken, folderAbsolutePath, templatesPath)
        logoutresponse = logout(url, bipSessionToken, templatesPath)
        deletefiles(auditfile)
        deletefiles(memberfile)

    # except Exception as e:
    #     log('Unknown Exception {}'.format(e))

    endTime = datetime.now()
    log('Started at: {}'.format(startTime))
    log('Finished at: {}'.format(endTime))
    log('Total time taken: {}'.format(endTime - startTime))
    logging.shutdown()
