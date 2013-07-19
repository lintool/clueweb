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

| Query        | Top 10        | Top 1000  |
| ------------- |:-------------:| ---------:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |

