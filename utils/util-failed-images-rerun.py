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


def remove_record(image_ids, image_info, sqldb, databasename,outdir):

    # Create results folder and make sure that outdir exists, if not, make it
    currentime = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    resultsdir = str(outdir) + "/" + str(currentime) + "_rerun"

    if os.path.exists(outdir) == False:
        os.mkdir(outdir)

    if os.path.exists(resultsdir) == False:
        os.mkdir(resultsdir)

    # Copy database and get tables
    newname=str(databasename)
    copyfile(sqldb,newname)
    newdatabase=newname
    con = sq.connect(newdatabase)
    con.text_factory = str
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
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
            if x1 == 'metadata':
                con = sq.connect(newdatabase)
                for y in image_ids:
                    query= "Select * from " + str(x1)+ " where image_id=" + str(y)
                    cursor = con.cursor()
                    cursor.execute(query)
                    metainfo = cursor.fetchall()
                    metainfo1 = list(metainfo[0])
                    headers=list(map(lambda x: x[0], cursor.description))
                    imagename="Select image from " + str(x1)+ " where image_id=" + str(y)
                    cursor.execute(imagename)
                    name = cursor.fetchall()
                    imgpath_split=str(name[0]).split("/")
                    name1 = re.sub(r'\W+', '', str(imgpath_split[-1]))
                    result_name = name1[:-3] + "_rerun.txt"
                    result_path = str(resultsdir) + "/" + result_name
                    result_file = open(result_path, 'w')
                    for i,x in enumerate(metainfo1):
                        result_file.write('META\t')
                        result_file.write(''+headers[i]+'\t')
                        result_file.write(''+str(metainfo1[i])+'\t\n')
                    result_file.close()
                con.close()

            for y in image_ids:
                query= "DELETE from " + str(x1)+ " where image_id=" + str(y)
                message="Currently deleting image_id="+str(y)+" from "+str(x1)
                print(message)
                con=sq.connect(newdatabase)
                cursor = con.cursor()
                cursor.execute(query)
                con.commit()
                con.close()
    return resultsdir

def create_jobfile(image_info, databasename,jobbase, resultsdir):
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

        # get indices of images that match run time
        image_array=np.asarray(image_info)
        image_match = np.where(image_array == timestamp)[0]
        print("A total of " + str(len(image_match)) + " jobs will be written into " + str(jobname) + " file")
        for index in image_match:
            line=image_array[index]
            image_path=line[0]
            result_name=image_path.split("/")[-1][:-4]+"_rerun.txt"
            result_path=str(resultsdir)+"/"+result_name
            argstr = (str(pipeline) + " -i " + str(image_path) + " -o " + str(resultsdir)) + " -w -r "+str(result_path)

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

    # This step finds the image_ids of the failed images, and the paths of the failed images
    img_ids,img_info = read_failed_images(args.directory)

    # This step can be slow depending on how many records need to be deleted, if you've already run this step it is
    # best to turn it off, if you just need to modify the created jobfiles.

    outfolder=remove_record(img_ids, img_info, args.database, args.databasename,args.outdir)

    # This step creates new job files for each run command (see runinfo table in the database).
    create_jobfile(img_info,args.databasename,args.job, outfolder)

if __name__ == '__main__':
    main()