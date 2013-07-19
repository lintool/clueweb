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
	-output /user/chauff/res.dir1000 \
	-queries /user/chauff/web.queries.trec2013 \
	-vbdocvector /data/private/clueweb12/derived/docvectors.20130710/segm*/part* \
	-topk 1000
``` 

The parameters are:
+ `dictionary`: HDFS path to the dictionary created by the clueweb tools
+ `smoothing`: the smoothing parameter in the LM-based retrieval model; a value of <=1 automatically backs off to smoothing with linear interpolation while a value >1 runs Dirichlet smoothing (default is 1000)
+ `output`: folder in which the TREC results are collected (in TREC result file format); to merge everything into one file in the end call `hadoop fs -getmerge /user/chauff/res.dir1000 res.dir1000`; the merged result file should run smoothly through `trec_eval`
+ `queries`: HDFS path to query file (assumed format is the same as this year's distributed query file, i.e. per line [queryID]:[term1] [term2] ...)
+ `vbdocvector`: HDFS path to the document vectors created by the clueweb tools; beware of the necessity for using `*` to identify the files (instead of just the folder)
+ `topk`: number of results that should be returned per query (default is 1000)


Retrieval runs
--------------
The file `runs/res.dir1000` contains the baseline result file when running the above retrieval program (i.e. LM with Dirichlet smoothing and mu=1000).
On an empty sara cluster, this run on 50 queries takes about one hour.


Sanity check
------------
To have confidence in the implementation, the baseline run is compared with the [official 2013 baselines](https://github.com/trec-web/trec-web-2013/tree/master/data/runs/baselines/2013/ql) (Indri runs) provided by the TREC's Web track organizers.

Since no relevance judgments are available for ClueWeb12, we report the overlap in document ids among the top 10 / top 1000 ranked documents for each query between our baseline and the organizer ql baseline [results-catA.txt](https://github.com/trec-web/trec-web-2013/blob/master/data/runs/baselines/2013/ql/results-cata.txt). The Perl script to compute the overlap is [available as well](https://github.com/chauff/clueweb/blob/master/scripts/computeOverlap.pl). 

For about half of the queries, the overlap is 50% or more, indicating the code to be viable (differences can be explained through differences in HTML parsing, tokenization, stopwording, etc.)

| Query        | Top 10 Overlap | Top 1000 Overlap  |
| ------------ |:-------------:| ------------------:|
| 201 | 90% | 84%   |
| 202 | 60% | 88%   |
| 203 | 60% | 66%   |
| 204 | 20% | 70%   |
| 205 | 30% | 46%   |
| 206 | 70% | 85%   |
| 207 | 0% |  15%  | 
| 208 | 60% | 89%   |
| 209 | 30% | 57%   |
| 210 | 50% | 81%   |
| 211 | 20% | 22%   |
| 212 | 30% | 46%   |
| 213 | 90% | 92%   |
| 214 | 60% | 67%   |
| 215 | 10% | 53%   |
| 216 | 20% | 50%   |
| 217 | 30% | 63%   |
| 218 | 50% | 59%   |
| 219 | 0% |  0%  |
| 220 | 10% | 24%   |
| 221 | 40% | 69%   |
| 222 | 90% | 73%   |
| 223 | 100% |81%    |
| 224 | 40% | 47%   |
| 225 | 80% | 88%   |
| 226 | 70% | 88%   |
| 227 | 0% |  3%  |
| 228 | 10% | 42%   |
| 229 | 60% | 80%   |
| 230 | 0% |  0%  |
| 231 | 0% |  28%  |
| 232 | 80% | 63%   |
| 233 | 70% | 86%   |
| 234 | 70% | 85%   |
| 235 | 60% | 76%   |
| 236 | 70% | 74%   |
| 237 | 90% | 52%   |
| 238 | 70% | 63%   |
| 239 | 70% | 92%   |
| 240 | 80% | 45%   |
| 241 | 0% |  2%  |
| 242 | 60% | 89%   |
| 243 | 40% | 82%   |
| 244 | 70% | 92%   |
| 245 | 50% | 78%   |
| 246 | 30% | 72%   |
| 247 | 80% | 56%   |
| 248 | 50% | 63%   |
| 249 | 0% |  2%  |
| 250 | 90% | 53%   |
