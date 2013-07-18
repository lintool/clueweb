ClueWeb Tools (Fork) - Retrieval models
=======================================

This is a collection of tools for manipulating the [ClueWeb12 collection](http://lemurproject.org/clueweb12/).

The code is forked (and updated) based on [Jimmy Lin's clueweb repository](https://github.com/lintool/clueweb).

Retrieval
---------

Implemented is currently only the basic language modeling approach to IR; smoothing types are linear interpolation or Dirichlet.

To run the code first follow the installation guideline of the original [clueweb repository]((https://github.com/lintool/clueweb) and build the dictionary and document vectors as described.


To conduct a retrieval run, call:

```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.LMRetrieval \
	-dictionary /data/private/clueweb12/derived/dictionary.20130710 \
	-smoothing 1000 \
	-output /user/claudiah/retrieved \
	-queries /user/claudiah/web.queries.trec2013 \
	-vbdocvector /data/private/clueweb12/derived/docvectors.20130710/segm*/part* \
	-topk 1000
``` 

The parameters are as follows
+ `dictionary`: HDFS path to the dictionary created by the clueweb tools
+ `smoothing`: the smoothing parameter in the LM-based retrieval model; a value of <=1 automatically backs off to smoothing with linear interpolation while a value >1 runs Dirichlet smoothing (default is 1000)
+ `output`: folder in which the TREC results are collected (in TREC format); to merge everything into one file in the end call `hadoop fs -getmerge /path/to/output filename`; the resulting file should run smoothly through `trec_eval`
+ `queries`: HDFS path to query file (assumed format is the same as this year's distributed query file, i.e. per line [queryID]:[term1] [term2] ...)
+ `vbdocvector`: HDFS path to the document vectors created by the clueweb tools; beware of the necessity for using `*` to identify the files (instead of just the folder)
+ `topk`: number of results that should be returned per query (default is 1000)


Retrieval runs
--------------
The folder `runs` contains the two baselines when running the above command with `smoothing 0.5` and `smoothing 1000` respectively.


Sanity check
------------
To have confidence in the implementation, these baseline runs where compared with the [official organizer baselines](https://github.com/trec-web/trec-web-2013/tree/master/data/runs/baselines/2013/ql) (an Indri run) provided by the TREC's Web track organizers.

