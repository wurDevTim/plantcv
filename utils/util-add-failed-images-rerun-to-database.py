#!/usr/bin/env python

import argparse
import numpy as np
from plantcv import plantcv as pcv
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
# record, to do this a deletion and jobfile creation script was made named 'util-failed-images-rerun.py'
# Once the 'util-failed-images-rerun.py' script has been run, and the jobfiles are executed/finished
# this script should be used to add the resulting files to the database.

### Parse command-line arguments
def options():
  parser = argparse.ArgumentParser(description="Get images from an SQLite database and some input information")
  parser.add_argument("-s", "--database", help="SQLite database file that has failed image records deleted", required=True)
  parser.add_argument("-d", "--directory", help="Path to directory containing results files.", required=True)
  args = parser.parse_args()
  return args


def add_to_database(database,directory):



    return new_database


### Main pipeline
def main():
    # Get options
    args = options()


if __name__ == '__main__':
    main()