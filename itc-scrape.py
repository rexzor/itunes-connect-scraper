#!/usr/bin/python

'''
Scraper for iTunes Connect. Will download the last day's daily report.
Bugfixes gratefully accepted. Send to jamcode <james@jam-code.com>.

* Copyright (c) 2009, jamcode LLC
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of the <organization> nor the
*       names of its contributors may be used to endorse or promote products
*       derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY <copyright holder> ''AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL <copyright holder> BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

import urllib, urllib2, sys, os, os.path, re, pprint, gzip, StringIO, getopt
import traceback
import time
from datetime import datetime, timedelta
from BeautifulSoup import BeautifulSoup
from pysqlite2 import dbapi2 as sqlite


baseURL = 'https://itts.apple.com'
refererURL = 'https://itts.apple.com/cgi-bin/WebObjects/Piano.woa'

# column mappings
id = 19
country_code = 14
r_date = 11
product_type = 8
units = 9
roalty_price = 10
roalty_currency = 15
customer_price = 20
customer_currency = 13
title = 6
vendor = 2
service_provider = 0
service_provider_country_code = 1
upc = 3
isrc = 4
artist = 5
label = 7
preorder = 16
season_pass = 17
isan = 18
cma = 21
asset = 22
vendor_offer_code = 23
grid = 24
promtion_code = 25
parent_id = 26

def updateReferer(opener, url) :
    refererURL = url
    opener.addheaders = [
        ('Referer', refererURL)
    ]

def logMsg(m, v) :
    if v :
        print >> sys.stderr, m

def getLastDayReport(username, password, reportDate, verbose=False) :
    logMsg('Initialising session with iTunes connect...', verbose)
    opener = urllib2.build_opener()
    s = opener.open(refererURL)
    updateReferer(opener, refererURL)
    logMsg('DONE', verbose)

    logMsg('Locating login form...', verbose)
    b = BeautifulSoup(s.read())
    form = b.findAll('form')[0]
    formArgs = dict(form.attrs)

    loginUrl = baseURL + formArgs['action']
    loginData = {
        'theAccountName' : username,
        'theAccountPW' : password,
        '1.Continue.x' : '36',
        '1.Continue.y' : '17',
        'theAuxValue' : ''
    }
    loginArgs = urllib.urlencode(loginData)
    logMsg('DONE', verbose)

    logMsg('Attempting to login to iTunes connect', verbose)
    h = opener.open(loginUrl, loginArgs)
    updateReferer(opener, loginUrl)

    b = BeautifulSoup(h.read())
    reportURL = baseURL + dict(b.findAll(attrs={'name' : 'frmVendorPage'})[0].attrs)['action']
    logMsg('DONE', verbose)

    logMsg('Fetching report form details...', verbose)
    reportTypeName = str(dict(b.findAll(attrs={'id' : 'selReportType'})[0].attrs)['name'])
    dateTypeName = str(dict(b.findAll(attrs={'id' : 'selDateType'})[0].attrs)['name'])

    '''
    Captured with Live HTTP Headers:
        9.7=Summary
        9.9=Daily
        hiddenDayOrWeekSelection=Daily
        hiddenSubmitTypeName=ShowDropDown
    '''

    reportData = [
        (reportTypeName, 'Summary'),
        (dateTypeName, 'Daily'),
        ('hiddenDayOrWeekSelection', 'Daily'),
        ('hiddenSubmitTypeName', 'ShowDropDown')
    ]

    h = opener.open(reportURL, urllib.urlencode(reportData))
    updateReferer(opener, reportURL)

    b = BeautifulSoup(h.read())
    reportURL = baseURL + dict(b.findAll(attrs={'name' : 'frmVendorPage'})[0].attrs)['action']

    # Don't know if these change between calls. Re-fetch them to be sure.
    reportTypeName = str(dict(b.findAll(attrs={'id' : 'selReportType'})[0].attrs)['name'])
    dateTypeName = str(dict(b.findAll(attrs={'id' : 'selDateType'})[0].attrs)['name'])
    dateName = str(dict(b.findAll(attrs={'id' : 'dayorweekdropdown'})[0].attrs)['name'])
    logMsg('DONE', verbose)


    logMsg("Fetching report for %s..." % reportDate, verbose)
    '''
    Captured with Live HTTP Headers:
        9.7=Summary
        9.9=Daily
        9.11.1=03%2F12%2F2009
        download=Download
        hiddenDayOrWeekSelection=03%2F12%2F2009
        hiddenSubmitTypeName=Download
    '''

    reportData = [
        (reportTypeName, 'Summary'),
        (dateTypeName, 'Daily'),
        (dateName, reportDate),
        ('download', 'Download'),
        ('hiddenDayOrWeekSelection', reportDate),
        ('hiddenSubmitTypeName', 'Download')
    ]

    h = opener.open(reportURL, urllib.urlencode(reportData))
    logMsg('DONE', verbose)

    logMsg('Decompressing report...', verbose)
    s = StringIO.StringIO(h.read())
    f = gzip.GzipFile(fileobj=s)
    logMsg('DONE', verbose)
    logMsg(h.read(), verbose)
    return f.read()

def usage(executableName) :
    print >> sys.stderr, "Usage: %s -u <username> -p <password> [-d mm/dd/year] [-o <database file>]" % executableName

def checkDatabaseSetup(connection) :
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS product_types (id VARCHAR(5) PRIMARY KEY, name VARCHAR(50))')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("1", "Application Purchase")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("7", "Application Update")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("IA1", "In-App Purchase")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("IAY", "In-App Subscription")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("1F", "Universal Apps, Free & Paid Apps")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("7F", "Universal Apps, Updates")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("1T", "iPad Only, Free & Paid Apps")')
    cursor.execute('INSERT OR IGNORE INTO product_types VALUES ("7T", "iPad Only, Updates")')
    cursor.execute('CREATE TABLE IF NOT EXISTS records (id INTEGER, country_code VARCHAR(3), r_date DATE, product_type VARCHAR(5), units INTEGER, roalty_price REAL, roalty_currency VARCHAR(3), customer_price REAL, customer_currency VARCHAR(3), title VARCHAR(255), vendor TEXT, service_provider VARCHAR(50), service_provider_country_code VARCHAR(3), upc TEXT, isrc TEXT, artist TEXT, label TEXT, preorder TEXT, season_pass TEXT, isan TEXT, cma TEXT, asset TEXT, vendor_offer_code TEXT, grid TEXT, promtion_code VARCHAR(10), parent_id INTEGER, PRIMARY KEY (id, country_code, r_date, product_type))')

def storeRecordsToDatabase(connection, records) :
    cursor = connection.cursor()
    for line in records.splitlines()[1:]:
    	values = line.split("\t")
	date = time.strptime(values[r_date], '%m/%d/%Y')
	date = time.strftime('%Y-%m-%d', date)
	cursor.execute('INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (values[id], values[country_code], date, values[product_type], values[units], values[roalty_price], values[roalty_currency], values[customer_price], values[customer_currency], values[title], values[ vendor], values[service_provider], values[service_provider_country_code], values[upc], values[isrc], values[artist], values[label], values[preorder], values[season_pass], values[isan], values[cma], values[asset], values[vendor_offer_code], values[grid], values[promtion_code], values[parent_id] ))
	

def main(args) :
    username, password, verbose, database = None, None, None, None
    try :
        opts, args = getopt.getopt(sys.argv[1:], 'vu:p:d:o:')
    except getopt.GetoptError, err :
        print >> sys.stderr, "Error: %s" % str(err)
        usage(os.path.basename(args[0]))
        sys.exit(2)

    # Get today's date by default. Actually yesterday's date
    reportDay = datetime.today() - timedelta(1)
    reportDate = reportDay.strftime('%m/%d/%Y')

    for o, a in opts :
        if o == '-u' : 
            username = a
        if o == '-p' :
            password = a
        if o == '-d' :
            reportDate = a
        if o == '-v' :
            verbose = True
	if o == '-o' :
	    database = a
    
    if None in (username, password) :
        print >> sys.stderr, "Error: Must set -u and -p options."
        usage(os.path.basename(args[0]))
        sys.exit(3)

    result = None
    if verbose :
        # If the user has specified 'verbose', just let the exception propagate
        # so that we get a stacktrace from python.
        result = getLastDayReport(username, password, reportDate, True)
    else :
        try :
            result = getLastDayReport(username, password, reportDate)
        except Exception, e :
            print >> sys.stderr, "Error: problem processing output. Check your username and password."
            print >> sys.stderr, "Use -v for more detailed information."

    print result
    if database != None :
        if verbose :
     	    print "Saving Data to DB."
	connection = sqlite.connect(database)
	checkDatabaseSetup(connection)
	storeRecordsToDatabase(connection, result)
	connection.commit()
	connection.close()
if __name__ == '__main__' :
    main(sys.argv)

