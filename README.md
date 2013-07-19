ClueWeb Tools (Fork) - Retrieval models
=======================================

This is a collection of tools for manipulating the [ClueWeb12 collection](http://lemurproject.org/clueweb12/).

The code is forked from [Jimmy Lin's clueweb repository](https://github.com/lintool/clueweb). The only change occured in the addition of a retrieval app

+ org.clueweb.clueweb12.app.LMRetrieval


Retrieval
---------

Implemented is currently the basic language modeling approach to IR; smoothing types are linear interpolation and Dirichlet.

To run the code first follow the installation guideline of the original [clueweb repository]((https://github.com/lintool/clueweb) and build the dictionary and document vectors as described.

To conduct a retrieval run, call:

```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.LMRetrieval \
	-dictionary /data/private/clueweb12/derived/dictionary.20130710 \
	-smoothing 1000 \
	-output /user/chauff/retrieved \
	-queries /user/chauff/web.queries.trec2013 \
	-vbdocvector /data/private/clueweb12/derived/docvectors.20130710/segm*/part* \
	-topk 1000
``` 

The parameters are:
+ `dictionary`: HDFS path to the dictionary created by the clueweb tools
+ `smoothing`: the smoothing parameter in the LM-based retrieval model; a value of <=1 automatically backs off to smoothing with linear interpolation while a value >1 runs Dirichlet smoothing (default is 1000)
+ `output`: folder in which the TREC results are collected (in TREC format); to merge everything into one file in the end call `hadoop fs -getmerge /path/to/output filename`; the resulting file should run smoothly through `trec_eval`
+ `queries`: HDFS path to query file (assumed format is the same as this year's distributed query file, i.e. per line [queryID]:[term1] [term2] ...)
+ `vbdocvector`: HDFS path to the document vectors created by the clueweb tools; beware of the necessity for using `*` to identify the files (instead of just the folder)
+ `topk`: number of results that should be returned per query (default is 1000)


Retrieval runs
--------------
The folder `runs` contains the baseline run when running the above retrieval program.

On an empty sara cluster, this run on 50 queries takes about one hour.


Sanity check
------------
To have confidence in the implementation, the baseline run was compared with the [official 2013 baselines](https://github.com/trec-web/trec-web-2013/tree/master/data/runs/baselines/2013/ql) (Indri runs) provided by the TREC's Web track organizers.

Since no relevance juddgments are available for ClueWeb12, we report the overlap in document ids among the top 10 / top 1000 ranked documents for each query between our baseline and the organizer ql baseline (results-catA.txt). For about half of the queries, the overlap is 50% or more, indicating the code to be viable (differences can be explained through differences in HTML parsing, tokenization, stopwording, etc.)

Percentage of overlap up to rank 10: 50
Query 201 => overlap: 90%
Query 202 => overlap: 60%
Query 203 => overlap: 60%
Query 204 => overlap: 20%
Query 205 => overlap: 30%
Query 206 => overlap: 70%
Query 207 => overlap: 0%
Query 208 => overlap: 60%
Query 209 => overlap: 30%
Query 210 => overlap: 50%
Query 211 => overlap: 20%
Query 212 => overlap: 30%
Query 213 => overlap: 90%
Query 214 => overlap: 60%
Query 215 => overlap: 10%
Query 216 => overlap: 20%
Query 217 => overlap: 30%
Query 218 => overlap: 50%
Query 219 => overlap: 0%
Query 220 => overlap: 10%
Query 221 => overlap: 40%
Query 222 => overlap: 90%
Query 223 => overlap: 100%
Query 224 => overlap: 40%
Query 225 => overlap: 80%
Query 226 => overlap: 70%
Query 227 => overlap: 0%
Query 228 => overlap: 10%
Query 229 => overlap: 60%
Query 230 => overlap: 0%
Query 231 => overlap: 0%
Query 232 => overlap: 80%
Query 233 => overlap: 70%
Query 234 => overlap: 70%
Query 235 => overlap: 60%
Query 236 => overlap: 70%
Query 237 => overlap: 90%
Query 238 => overlap: 70%
Query 239 => overlap: 70%
Query 240 => overlap: 80%
Query 241 => overlap: 0%
Query 242 => overlap: 60%
Query 243 => overlap: 40%
Query 244 => overlap: 70%
Query 245 => overlap: 50%
Query 246 => overlap: 30%
Query 247 => overlap: 80%
Query 248 => overlap: 50%
Query 249 => overlap: 0%
Query 250 => overlap: 90%


ercentage of overlap up to rank 1000: 62%
Query 201 => overlap: 84%
Query 202 => overlap: 87.7%
Query 203 => overlap: 66%
Query 204 => overlap: 70.3%
Query 205 => overlap: 45.7%
Query 206 => overlap: 85.3%
Query 207 => overlap: 14.8%
Query 208 => overlap: 89%
Query 209 => overlap: 57.4%
Query 210 => overlap: 80.9%
Query 211 => overlap: 22%
Query 212 => overlap: 46%
Query 213 => overlap: 91.6%
Query 214 => overlap: 66.8%
Query 215 => overlap: 53.1%
Query 216 => overlap: 50.4%
Query 217 => overlap: 62.6%
Query 218 => overlap: 59%
Query 219 => overlap: 0%
Query 220 => overlap: 24%
Query 221 => overlap: 68.6%
Query 222 => overlap: 72.8%
Query 223 => overlap: 80.6%
Query 224 => overlap: 46.7%
Query 225 => overlap: 88%
Query 226 => overlap: 87.9%
Query 227 => overlap: 2.8%
Query 228 => overlap: 41.6%
Query 229 => overlap: 79.8%
Query 230 => overlap: 0%
Query 231 => overlap: 28.1%
Query 232 => overlap: 62.8%
Query 233 => overlap: 86.2%
Query 234 => overlap: 85.1%
Query 235 => overlap: 75.9%
Query 236 => overlap: 73.8%
Query 237 => overlap: 51.5%
Query 238 => overlap: 63.1%
Query 239 => overlap: 92.4%
Query 240 => overlap: 45.1%
Query 241 => overlap: 1.6%
Query 242 => overlap: 88.6%
Query 243 => overlap: 81.2%
Query 244 => overlap: 91.5%
Query 245 => overlap: 78.2%
Query 246 => overlap: 72%
Query 247 => overlap: 56.3%
Query 248 => overlap: 62.5%
Query 249 => overlap: 1.5%
Query 250 => overlap: 53%

