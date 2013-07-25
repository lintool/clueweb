ClueWeb Tools (Fork) - Retrieval models
=======================================

The code is forked from [Jimmy Lin's clueweb repository](https://github.com/lintool/clueweb). The only change occured in the addition of a retrieval app

Retrieval
---------

Implemented is currently the basic language modeling approach to IR; smoothing types are linear interpolation and Dirichlet.

To run the code first follow the installation guideline of the original [clueweb repository]((https://github.com/lintool/clueweb) and build the dictionary and document vectors as described.

To conduct a retrieval run, call:

```
$ hadoop jar clueweb-tools-X.X-SNAPSHOT-fatjar.jar \
	org.clueweb.clueweb12.app.LMRetrieval \
	-dictionary /data/private/clueweb12/derived/dictionary.XXX \
	-smoothing 1000 \
	-output /user/chauff/res.dir1000 \
	-queries /user/chauff/web.queries.trec2013 \
	-docvector /data/private/clueweb12/derived/docvectors.XXX/*/part* \
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
+ `preprocessing`: indicates the tokenizaton/stemming procedure; either `porter` or `standard` at the moment; needs to be in line with the dictionary/docvector


Retrieval runs
--------------
The files `runs/res.dir1000.{standard,porter}` contain the baseline results when running the above retrieval program (i.e. LM with Dirichlet smoothing and mu=1000) with `standard` and `porter` preprocessing respectively.
On an empty sara cluster, this run on 50 queries takes about one hour.


Sanity check
------------
To have confidence in the implementation, the baseline runs are compared with the [official 2013 baselines](https://github.com/trec-web/trec-web-2013/tree/master/data/runs/baselines/2013/ql) (Indri runs) provided by the TREC's Web track organizers.

Since no relevance judgments are available for ClueWeb12, we report the overlap in document ids among the top 10 / top 1000 ranked documents for each query between our baseline and the organizer ql baseline [results-catA.txt](https://github.com/trec-web/trec-web-2013/blob/master/data/runs/baselines/2013/ql/results-cata.txt). The Perl script to compute the overlap is [available as well](https://github.com/chauff/clueweb/blob/master/scripts/computeOverlap.pl). 

The organizer's baseline was run with Krovetz stemming, so we expect our Porter-based to have higher overlap than the `standard` run. This is indeed the case. The few 0% queries can be explained by the different tokenization, HTML parsing and the different stemming approaches (Porter is more agressive than Krovetz). 


| Query | Top 10 Overlap | Top 1000 Overlap|Top 10 Overlap|Top 1000 Overlap|
|----|----|----|----|----|
| 201 | 90% | 84%   |90%|85%|
| 202 | 60% | 88%   |70%|88%|
| 203 | 60% | 66%   |70%|73%|
| 204 | 20% | 70%   |70%|83%|
| 205 | 30% | 46%   |60%|70%|
| 206 | 70% | 85%   |70%|87%|
| 207 | 0% |  15%  | 0%|15%|
| 208 | 60% | 89%   |60%|91%|
| 209 | 30% | 57%   |80%|81%|
| 210 | 50% | 81%   |70%|83%|
| 211 | 20% | 22%   |50%|52%|
| 212 | 30% | 46%   |60%|86%|
| 213 | 90% | 92%   |90%|95%|
| 214 | 60% | 67%   |100%|83%|
| 215 | 10% | 53%   |20%|60%|
| 216 | 20% | 50%   |60%|82%|
| 217 | 30% | 63%   |40%|58%|
| 218 | 50% | 59%   |80%|89%|
| 219 | 0% |  14%  |0%|15%|
| 220 | 10% | 24%   |40%|67%|
| 221 | 40% | 69%   |60%|71%|
| 222 | 90% | 73%   |100%|88%|
| 223 | 100% |81%    |100%|86%|
| 224 | 40% | 47%   |40%|59%|
| 225 | 80% | 88%   |80%|83%|
| 226 | 70% | 88%   |70%|88%|
| 227 | 0% |  3%  |0%|5%|
| 228 | 10% | 42%   |4057%%|
| 229 | 60% | 80%   |90%|91%|
| 230 | 0% |  29%  |50%|28%|
| 231 | 0% |  28%  |30%|27%|
| 232 | 80% | 63%   |80%|74%|
| 233 | 70% | 86%   |70%|94%|
| 234 | 70% | 85%   |80%|89%|
| 235 | 60% | 76%   |60%|84%|
| 236 | 70% | 74%   |80%|84%|
| 237 | 90% | 52%   |80%|60%|
| 238 | 70% | 63%   |70%|68%|
| 239 | 70% | 92%   |90%|93%|
| 240 | 80% | 45%   |80%|75%|
| 241 | 0% |  2%  |0%|33%|
| 242 | 60% | 89%   |100%|94%|
| 243 | 40% | 82%   |60%|84%|
| 244 | 70% | 92%   |80%|92%|
| 245 | 50% | 78%   |30%|83%|
| 246 | 30% | 72%   |80%|81%|
| 247 | 80% | 56%   |90%|60%|
| 248 | 50% | 63%   |90%|87%|
| 249 | 0% |  2%  |0%|4%|
| 250 | 90% | 53% |90%|57%|
