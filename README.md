ClueWeb Tools (Fork) - Retrieval models
=======================================

The code is forked from [Jimmy Lin's clueweb repository](https://github.com/lintool/clueweb). This repository focuses on additional apps (for retrieval, spam filtering, duplicate filtering, etc.)


Important Notes
---------------
+ The apps `BuildPForDocVectors`, `BuildVByteDocVectors` and `ComputeTermStatistics` require an additional parameter: `htmlParser` which can either be `tika` or `jsoup` (changes the HTML parser used).
+ The parameters `htmlParser` and `preprocessing` need to be the same during the creation of the dictionary and document vectors and the retrieval apps (otherwise the results will be poor).


Retrieval
---------

Implemented are currently the basic language modeling approach to IR and its pseudo-relevance feedback component (relevance language models RM1/RM3).
The implemented smoothing types are linear interpolation and Dirichlet.

To run the code first follow the installation guideline of the original [clueweb repository]((https://github.com/lintool/clueweb) and build the dictionary and document vectors as described.

To conduct a query likelihood retrieval run, call:

```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.LMRetrieval \
	-dictionary /data/private/clueweb12/derived/dictionary.XXX \
	-docvector /data/private/clueweb12/derived/docvectors.XXX/*/part* \
	-smoothing 1000 \
	-output /user/chauff/res.dir1000 \
	-queries /user/chauff/web.queries.trec2013 \
	-topk 1000 \
	-preprocessing porter
``` 

The parameters are:
+ `dictionary`: HDFS path to the dictionary created by the clueweb tools
+ `smoothing`: the smoothing parameter in the LM-based retrieval model; a value of <=1 automatically backs off to smoothing with linear interpolation while a value >1 runs Dirichlet smoothing (default is 1000)
+ `output`: folder in which the TREC results are collected (in TREC result file format); to merge everything into one file in the end call `hadoop fs -getmerge /user/chauff/res.dir1000 res.dir1000`; the merged result file should run smoothly through `trec_eval`
+ `queries`: HDFS path to query file (assumed format is the same as this year's distributed query file, i.e. per line [queryID]:[term1] [term2] ...)
+ `docvector`: HDFS path to the document vectors (PFor format) created by the clueweb tools; beware of the necessity for using `*` to identify the files (instead of just the folder)
+ `topk`: number of results that should be returned per query (default is 1000)
+ `preprocessing`: indicates the stemming procedure; either `porter`, `krovetz` or `standard` (no stemming)


To create a RM1/RM3 retrieval run, two steps are necessary: 
+ first, an updated query model is created (derived from the top ranked documents of the query likelihood run)
+ the updated query model is used for retrieval

The call to create the query model based on the initially top ranked documents is as follows:
```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.RMModel \
	-dictionary /data/private/clueweb12/derived/dictionary.XXX \
	-docvector /data/private/clueweb12/derived/docvectors.XXX/*/part* \
	-smoothing 1000 \
	-output /user/chauff/rmmodel \
	-trecinputfile /user/chauff/res.dir1000 \
	-numFeedbackDocs 10 \
	-numFeedbackTerms 10 \
``` 

The additional parameters are:
+ `trecinputfile`: the result file generated in the query likelihood run
+ `numFeedbackDocs`: the number of top ranked documents per query that are used to compute the updated query model
+ `numFeedbackTerms`: the number of terms retained in the query model

The `output` file of the app contains the query model and has the format `[queryID] [term] [weight]`:
```
201	boards	0.011376762
201	video	0.0120078195
```

In the second step, the query model is interpolated with the original query:
```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.RMRetrieval \
	-dictionary /data/private/clueweb12/derived/dictionary.XXX \
	-docvector /data/private/clueweb12/derived/docvectors.XXX/*/part* \
	-smoothing 1000 \
	-topk 1000 \
	-preprocessing porter \
	-rmmodel /user/chauff/rmmodel \
	-queries /user/chauff/web.queries.trec2013 \
	-queryLambda 0.6 \
	-output /user/chauff/res.rm.dir1000
``` 

The additional parameters are:
+ `rmmodel`: the file generated in the first step
+ `queryLambda`: how to interpolate the query model with the original query (if set to 0, the language model type is RM1, otherwise RM3)
+ `output`: the result file in TREC format



Spam Filter
-----------

The [spam scores provided by UWaterloo](http://www.mansci.uwaterloo.ca/~msmucker/cw12spam/) are also available on sara.

The spam filtering app takes as input a TREC result file (generated by the retrieval app for instance) and filters out all documents with a spam score below a certain threshold. 
Spam scores can be between 0 and 99 with the spammiest documents having a score of 0 and the most non-spam documents having a score of 99. Using 70 as threshold usually works well.

To run the code, call:

```
$ hadoop jar clueweb-tools-0.3-SNAPSHOT-fatjar.jar
	org.clueweb.clueweb12.app.SpamScoreFiltering \
	-output /user/chauff/res.dir1000.porter.spamFiltered \
	-spamScoreFolder /data/private/clueweb12/derived/waterloo-spam-cw12-decoded \ 		
	-spamThreshold 70 \
	-trecinputfile /user/chauff/res.dir1000.porter \
``` 

The parameters are:
+ `output`: folder in which the TREC results are collected (in TREC result file format)
+ `spamScoreFolder`: HDFS path to the folder where the UWaterloo spam scores reside
+ `spamThreshold`: documents with a spam score BELOW this number are considered as spam
+ `trecinputfile`: HDFS path to the TREC result file which is used as starting point for filtering



De-duplication
--------------
To increase diversity, duplicate documents can be removed from the result ranking (in effect pushing lower ranked results up the ranking).

A simple cosine based similarity approach is implemented in `DuplicateFiltering`: every document at rank x is compared to all non-duplicate documents at higher ranks. If its cosine similarity is high enough, it is filtered out.

To run the code, call:

```
$ hadoop jar clueweb-tools-0.3-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.DuplicateFiltering 
	-cosineSimThreshold 0.8 \ 
	-dictionary /data/private/clueweb12/derived/dictionary.XXX \
	-docvector /data/private/clueweb12/derived/docvectors.XXX/*/part* \
	-output /user/chauff/res.dir1000.porter.deduplicated \
	-topk 1000 \
	-trecinputfile /user/chauff/res.dir1000.porter
```

The parameters (apart from the usual ones) are:
+ `cosineSimThreshold`: documents having a cosine similarity above this threshold are removed from the result file
+ `trecinputfile`: file in TREC result format which is used as a starting point for de-duplication


Document extraction
-------------------

A helper app: given a file with a list of docids, it extracts the documents' content from the WARC files.

To run the code, call:

```
$ hadoop jar clueweb-tools-0.3-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.DocumentExtractor \
	-docidsfile /user/chauff/docids \
	-input /data/private/clueweb12/Disk*/*/*/*.warc.gz \
	-keephtml false \
	-output /user/chauff/docids-output
```

The parameters are:
+ `docidsfile`: a file with one docid per line; all docids are extracted from the WARC input files
+ `input`: list of WARC files
+ `keephtml`: parameter that is either `true` (keep the HTML source of each document) or `false` (parse the documents, remove HTML)
+ `output`: folder where the documents' content is stored - one file per docid


Retrieval runs
--------------
The files `runs/res.dir1000.{standard,porter}` contain the baseline results when running the above retrieval program (i.e. LM with Dirichlet smoothing and mu=1000) with `standard` and `porter` preprocessing respectively.
On an empty sara cluster, this run on 50 queries takes about one hour.

The file `runs/res.dir1000.porter.spamFiltered` is based on `runs/res.dir1000.porter` and filters out all documents with a spam score below 70.


Effectiveness on 2012/13 TREC Web queries
-----------------------------------------
To compare the implementation against an established baseline, we compare against the [Indri baseline](https://github.com/trec-web/trec-web-2013/tree/master/data/runs/baselines), provided by the TREC Web track organizers in 2012 for ClueWeb09 and in 2013 for ClueWeb12. For simplicity, we use MAP and NDCG@20 as metrics (although these are not the main metrics for the recent Web track tasks).

Our setup: Krovetz stemming, LM with Dirichlet smoothing (mu=1000), and RM with 100 feedback documents, 25 feedback terms and queryLambda of 0.6 -- this may not be fully aligned with the organizer baseline runs.

ClueWeb09 English part, queries 151 - 200, top 1000 documents
* Indri QL: 0.0512 (MAP), 0.0631 (NDCG@20)
* Indri RM: 0.0547 (MAP), 0.0618 (NDCG@20)

* this implementation QL: 0.0509 (MAP), 0.0444 (NDCG@20)
* this implementation RM: 0.0727 (MAP), 0.0641 (NDCG@20)

ClueWeb12, queries 201 - 250, top 10000 documents
* Indri QL: 0.1373 (MAP), 0.2153 (NDCG@20)
* Indri RM: 0.1208 (MAP), 0.1939 (NDCG@20)

* this implementation QL: 0.1404 (MAP), 0.2369 (NDCG@20)
* this implementation RM: 
