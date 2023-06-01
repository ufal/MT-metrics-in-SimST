import pandas as pd
import numpy as np



import csv
import logging
import math
import sys

LOG = logging.getLogger(__name__)

def isNaN(num):
    return num != num

def anot_fun(d,maxlen=float("inf")):
    grade_lens=[0 for _ in range(5)]
    d = d.sort_values("offset")
    last_o = None
    last_r = None
    last_row = None
    for i,r in d.iterrows():
        o, t = r.offset, r.rating
        if isNaN(t):
            t = 0
        doclen = r.length
        o = min(doclen, o)
        if last_o is not None:
            if o < last_o:
                print(r)
            this_len = min(o - last_o, maxlen)
            try:
                grade_lens[int(last_r)] += this_len
            except:
                print(last_row, r)
                raise
        last_o = o
        last_r = t
        last_row = r

    this_len = min(r.length - last_o, maxlen)
    if r.length < last_o:
        print("last")
    grade_lens[int(last_r)] += this_len
    return np.array(grade_lens)

def weighted_average(X):
    s = sum(X[1:])
    return sum(x*i for i,x in enumerate(X))/s


def weighted_avg_fun(d,maxlen=float("inf")):
    s = anot_fun(d, maxlen=maxlen)
    length = set(d.length).pop()
    
    return weighted_average(s), length

def doc_len_weighted_avg(d):
    s = 0
    ws = 0
    for a,b in d:
        s += b
        ws += a*b
    return ws/s

#def CR_weighted(data,maxlen=float("inf")):
#    return data.groupby(["system", "latency", "common", "annotator", "doc"]
#                       ).apply(weighted_avg_fun,maxlen=maxlen).groupby(["system", "latency", "common"]
#                                                                      ).apply(doc_len_weighted_avg)
#

def avg2(d):
    return [sum(a for a,_ in d)/len(d), sum(b for _,b in d)/len(d)]

def CR_weighted(data,maxlen=float("inf")):
    return data.groupby(["system", "latency", "common", "annotator", "doc"]
                       ).apply(weighted_avg_fun,maxlen=maxlen).groupby(["system", "latency", "common", "doc"]).apply(avg2
        ).groupby(["system", "latency", "common"]).apply(doc_len_weighted_avg)

### from Barry:

def read_data(csv_file):
  annotators, systems, latencies, docs, lengths, offsets, ratings, run_ids = [], [], [], [], [], [], [], []
  with open(csv_file) as fh:
    reader = csv.DictReader(fh, quotechar="'")
    for row in reader:
      if row['rating'] == "\\N":
        continue
      fields = row['subtitles'].split(".")
      if fields[0] == "interpreting":
        system,latency,doc = fields[0],"",fields[1]
      else:
        system,latency,doc = fields[0], fields[1], fields[2]
      annotator = int(row['annotator_id'])
      run_id = int(row['id'])
      length = float(row['audio_length'])
      for offset, rating in  eval(row['rating'])[1:]:
        offsets.append(int(offset))
        # A rating of 0 means that the annotator lost attention, so we ignore it
        if rating == 0:
            ratings.append(math.nan)
        else:
            ratings.append(int(rating))
        annotators.append(annotator)
        docs.append(doc)
        latencies.append(latency)
        systems.append(system)
        lengths.append(length)
        run_ids.append(run_id)

  data = pd.DataFrame({
    'run_id' : run_ids, # corresponds to id on original data
    "system" : systems,
    "latency" : latencies,
    "annotator" : annotators,
    "doc" : docs,
    "length" : lengths,
    "offset" : offsets,
    "rating" : ratings,

  })
  data['common'] = data['doc'].str.startswith("ted")

  # Data fixes

  LOG.debug(f"Total number of raw ratings: {len(data)}")

  # Remove any ratings which were more than 20 seconds past the end of the audio
  data['diff'] = data['offset'] - data['length']
  data.drop(data[data['diff'] > 20000].index, inplace = True)
  LOG.debug(f"Total ratings after removing late ratings: {len(data)}")


  # Remove runs where the annotator made too few annotations.
  # We expect at least length / 5 sec annotations, so if less than 1 per 20 secs, remove
  data_by_run = data.groupby(["run_id"])
  annot_density = data_by_run['length'].mean() / data_by_run['length'].count()
  #print(50000, len(annot_density[annot_density > 50000]))
  short_runs = annot_density[annot_density > 20000].reset_index()
  data.drop(data[data['run_id'].isin(short_runs['run_id'])].index, inplace=True)
  LOG.debug(f"Total ratings after removing partially rated audios: {len(data)}")
  return data


####

def load_CR_weighted(maxlen=float("inf")):
    data = read_data("data-20220507.csv")
    return CR_weighted(data,maxlen=maxlen)
