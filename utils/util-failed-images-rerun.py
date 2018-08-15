#!/usr/bin/env python

import argparse
import numpy as np
import os
import sqlite3 as sq
from shutil import copyfile
import datetime
import re
import pandas as pd
import fnmatch

# There was a bug in the plantcv-pipeline parallelization script.
# This bug had to do with how the jobs were divided per CPU.
# Once the jobs were divided by number of CPUs the remainder
# should have been added to one of the CPUs, but they were not.
# Therefore, a lot of 'failed' images failed simply because they
# were in the remainder and not because they should have actually failed.
# The bug has been fixed in plantcv-pipeline, but for previous runs we
# need to be able to rerun the failed images and remove and replace the database
# record.

### Parse command-line arguments
def options():
  parser = argparse.ArgumentParser(description="Get images from an SQLite database and some input information")
  parser.add_argument("-s", "--database", help="SQLite database file from plantcv.", required=True)
  parser.add_argument("-d", "--directory", help="path to directory containing failed images files.", required=True)
  parser.add_argument("-n", "--databasename", help="path and new name for altered database.", required=True)
  parser.add_argument("-o", "--outdir", help="out directory path.", required=True)
  parser.add_argument("-j", "--job", help="path and new name for altered database.", required=True)
  args = parser.parse_args()
  return args


def read_failed_images(directory):
    list=[]
    for file in os.listdir(directory):
        if fnmatch.fnmatch(file, "*_failed_*") & file.endswith(".log"):
            file1=str(directory)+str(file)
            list.append(file1)
    image_ids=[]
    image_info=[]
    for x in list:
        path=x.split("/")
        filename=str(path[-1])
        splitfile=filename.split("_")
        datelog=str(splitfile[-1])
        date=datelog.split(".")
        errorfiletime=splitfile[-2]+"_"+date[0]
        records=pd.read_table(x, header=None, delimiter='|')
        id=records[0].tolist()
        img=records[16].tolist()
        for item in id:
            image_ids.append(item)
        for item in img:
            info=[item,errorfiletime]
            image_info.append(info)
    return image_ids, image_info


def remove_record(image_ids, sqldb, databasename):
    newname=str(databasename)
    copyfile(sqldb,newname)
    newdatabase=newname
    con = sq.connect(newdatabase)
    con.text_factory = str
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(tables)
    con.close()
    tables1= [x for x in tables if 'runinfo' not in x]
    tables1= [x for x in tables1 if 'analysis_images' not in x]
    for x in tables1:
        x = str(x)
        x1 = re.sub(r'\W+', '', x)
        count = "SELECT count(*) from "+str(x1)
        con = sq.connect(newdatabase)
        cursor = con.cursor()
        cursor.execute(count)
        tablecount=cursor.fetchall()
        tablecount1 = re.sub(r'\W+', '', str(tablecount))
        tablecount1=(int(tablecount1))
        if tablecount1==0:
            pass
        else:
            for y in image_ids:
                query= "DELETE from " + str(x1)+ " where image_id=" + str(y)
                message="Currently deleting image_id="+str(y)+" from "+str(x1)
                print(message)
                con=sq.connect(newdatabase)
                cursor = con.cursor()
                cursor.execute(query)
                con.commit()
                con.close()
    return databasename

def create_jobfile(image_info, databasename,jobbase, outdir):
    con = sq.connect(databasename)
    cursor = con.cursor()
    cursor.execute("SELECT * FROM 'runinfo';")
    tables = cursor.fetchall()
    con.close()
    for i,x in enumerate(tables):
        timestamp=x[1]
        command=x[2]
        command_split=command.split(" ")
        pipeline_index=(command_split.index("-p"))+1
        pipeline=command_split[pipeline_index]
        exe =command_split[0]
        jobname=str(jobbase)+"_"+str(i)+".job"
        jobfile = open(jobname, 'w')
        currentime = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        resultsdir=str(outdir)+"/"+str(currentime)+"_rerun"

        if os.path.exists(outdir)==False:
            os.mkdir(outdir)

        if os.path.exists(resultsdir)==False:
            os.mkdir(resultsdir)

        # get indices of images that match run time
        image_array=np.asarray(image_info)
        image_match = np.where(image_array == timestamp)[0]
        print("A total of " + str(len(image_match)) + " jobs will be written into " + str(jobname) + " file")
        for index in image_match:
            line=image_array[index]
            image_path=line[0]
            result_name=image_path.split("/")[-1][:-4]+"_rerun.txt"
            result_path=str(resultsdir)+"/"+result_name
            argstr = (str(pipeline) + " -i " + str(image_path) + " -o " + str(outdir)) + " -w -r "+str(result_path)

            jobfile.write('####################\n')
            jobfile.write('# HTCondor job description file\n')
            jobfile.write('####################\n\n')
            jobfile.write('universe         = vanilla\n')
            jobfile.write('executable       = ' + exe + '\n')
            jobfile.write('arguments        = ' + argstr + '\n')
            jobfile.write('log              = $(Process).log\n')
            jobfile.write('output           = $(Process).out\n')
            jobfile.write('error            = $(Process).error\n')
            jobfile.write('request_cpus     = 1\n')
            jobfile.write('notification     = never\n')
            jobfile.write('nice_user        = False\n')
            jobfile.write('accounting_group = $ENV(CONDOR_GROUP)\n')
            jobfile.write('getenv           = true\n')
            jobfile.write('####################\n\n')
            jobfile.write('queue\n\n')
        jobfile.close()


### Main pipeline
def main():
    # Get options
    args = options()

    image_ids,img_info = read_failed_images(args.directory)
    newdatabasepath=remove_record(image_ids,args.database, args.databasename)
    create_jobfile(img_info,args.databasename,args.job, args.outdir)

if __name__ == '__main__':
    main()