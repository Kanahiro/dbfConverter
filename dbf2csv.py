#!/usr/bin/python
#coding:utf-8

import csv,os,sys
#to be able to deal a huge Database
csv.field_size_limit(1000000000)
from dbfpy import dbf

filename = sys.argv[1]
if filename.endswith('.dbf'):
    print ("Converting %s to csv" % filename)
    csv_fn = filename[:-4]+ ".csv"
    with open(csv_fn,'wb') as csvfile:
        in_db = dbf.Dbf(filename)
        out_csv = csv.writer(csvfile)
        names = []
        for field in in_db.header.fields:
            names.append(field.name)
        out_csv.writerow(names)
        for rec in in_db:
            out_csv.writerow(rec.fieldData)
        in_db.close()
        print("Done...")
elif filename.endswith('.csv'):
  print("Converting %s to dbf" % filename)
  csv_fn = filename[:-4]+ ".csv"
  with open(csv_fn, 'rb') as in_csv:
    reader = csv.reader(in_csv)
    out_db = dbf.Dbf(filename[:-4]+'.dbf', new=True)
    #make database-fields by CSV header
    header = next(reader)
    for col in header:
      out_db.addField((col,'C',20))
    for row in reader:
      rec = out_db.newRecord()
      for i in range(len(header)):
        rec[header[i]] = row[i]
      rec.store()
    out_db.close()
else:
  print("Filename does not end with .dbf or .csv")
