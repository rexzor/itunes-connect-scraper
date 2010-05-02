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
import calendar
from datetime import date
from xlwt import Workbook
from pysqlite2 import dbapi2 as sqlite

def logMsg(m, v) :
    if v :
        print >> sys.stderr, m

def writeOverviewReport(connection, sheet, year, x , y) :
    sheet.write(x, y, 'Month')
    cursor = connection.cursor()
    cursor.execute('select * from product_types')
    types = cursor.fetchall()
    for i in range(len(types)) :
        sheet.write(x, y + i +1, types[i][1])
    for m in range(1,13) :
        monthdate = date(year, m, 1)
        cursor.execute("select product_type, sum(units) from records where " +
           "r_date >= ? AND r_date <= ? GROUP BY strftime('%m', r_date), " + 
           "product_type", (monthdate.strftime('%Y-%m-%d'), date(year, m,  
           calendar.monthrange(year, m)[1]).strftime('%Y-%m-%d')))
        sheet.write(x + m, y, monthdate.strftime('%B'))
        r = cursor.fetchall()
        for type in types :
            if len(r) > 0 :
                if r[0][0] == type[0] : 
                    sheet.write(x+m, y+1+types.index(type), r[0][1])
                else :
                    sheet.write(x+m, y+1+types.index(type), 0)
            else :
                sheet.write(x+m, y+1+types.index(type), 0)

def writeMonthReport(connection, monthly_sheet, id, year, month, x, y) :
    monthly_sheet.write(x,y, 'Day')
    cursor = connection.cursor()
    cursor.execute('select * from product_types')
    types = cursor.fetchall()
    
    for type in types :
            monthly_sheet.write(x, y + types.index(type) +1, type[1])
    
    for d in range(1,calendar.monthrange(year, month)[1] + 1) :
        cdate = date(year, month, d)
        monthly_sheet.write(x+d, y, cdate.strftime("%d"))
        for type in types :
            cursor.execute("select sum(units) from records where " +
               "r_date = ? AND product_type = ? AND id = ?", 
               (cdate.strftime('%Y-%m-%d'), type[0], id))
            result = cursor.fetchone()
            if (result[0] != None) :
                monthly_sheet.write(x+d, y + types.index(type) + 1, result[0])
            else :
                monthly_sheet.write(x+d, y + types.index(type) + 1, 0)
        
        
            

def writeReport(connection, year, output) :
    cursor = connection.cursor()
    workbook = Workbook()
    overview_sheet = workbook.add_sheet('Overview')
    
    writeOverviewReport(connection, overview_sheet, year, 0, 0)
    
    products = cursor.execute('SELECT id, title from records GROUP BY id').fetchall()
    for product in products :
        for m in range(1,13) :
            monthdate = date(2010, m, 1)
            monthly_sheet = workbook.add_sheet(product[1] + " " + monthdate.strftime('%B'))
            writeMonthReport(connection, monthly_sheet, product[0], year, m, 0, 0)
    workbook.save(output)
     
    
def usage(executableName) :
    print >> sys.stderr, "Usage: %s -i <database file> [-v]" % executableName

def main(args) :
    database, verbose, year, output = None, None, None, None
    try :
        opts, args = getopt.getopt(sys.argv[1:], 'i:o:y:v')
    except getopt.GetoptError, err :
        print >> sys.stderr, "Error: %s" % str(err)
        usage(os.path.basename(args[0]))
        sys.exit(2)

    for o, a in opts :
        if o == '-v' :
            verbose = True
        if o == '-i' :
            database = a
        if (o == '-y') :
            year = int(a)
        if (o == '-o') :
            output = a
    
    if database == None :
        print >> sys.stderr, "Error: Must set -i option."
        usage(os.path.basename(args[0]))
        sys.exit(3)
    connection = sqlite.connect(database)
    writeReport(connection, year, output)
    connection.close()
if __name__ == '__main__' :
    main(sys.argv)

