import csv

import dataset
from datafreeze.app import freeze
from normality import slugify

db = dataset.connect("sqlite:///tweets.db")
result = db["tweets"].all()
freeze(result,format='csv',filename='tweets_file1.csv')