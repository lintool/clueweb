ClueWeb Tools
=============

This is a collection of tools for manipulating the [ClueWeb12 collection](http://lemurproject.org/clueweb12/).


Getting Stated
--------------

You can clone the repo with the following command:

```
$ git clone git://github.com/lintool/clueweb.git
``` 

Once you've cloned the repository, build the package with Maven:

```
$ mvn clean package appassembler:assemble
```

Two notes:

+ `appassembler:assemble` automatically generates a few launch scripts for you.
+ the package uses the [Maven Shade plugin](http://maven.apache.org/plugins/maven-shade-plugin/) to create a "fat jar" that includes all dependencies except for Hadoop, so the jar in `target/` can be directly submitted via `hadoop jar ...`.

To automatically generate project files for Eclipse:

```
$ mvn eclipse:clean
$ mvn eclipse:eclipse
```

You can then use Eclipse's Import "Existing Projects into Workspace" functionality to import the project.

Counting Records
----------------

For sanity checking and as a "template" for other Hadoop jobs, the package provides a simple program to count WARC records in ClueWeb12:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.app.CountClueWarcRecords -input /path/to/warc/files/
```

Examples of `/path/to/warc/files/` are:

+ `/data/private/clueweb12/Disk1/ClueWeb12_00/*/*.warc.gz`: for a single ClueWeb12 segment
+ `/data/private/clueweb12/Disk1/ClueWeb12_*/*/*.warc.gz`: for an entire ClueWeb12 disk
+ `/data/private/clueweb12/Disk[1234]/ClueWeb12_*/*/*.warc.gz`: for all of ClueWeb12

Building a Dictionary
---------------------

The next step is to build a dictionary that provides three capabilities:

+ a bidirectional mapping from terms (strings) to termids (integers)
+ lookup of document frequency (*df*) by term or termid
+ lookup of collection frequency (*cf*) by term or termid

To build the dictionary, we must first compute the term statistics:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.app.ComputeTermStatistics \
 -input /data/private/clueweb12/Disk1/ClueWeb12_00/*/*.warc.gz \
 -output term-stats/segment00
```

By default, the program throws away all terms with *df* less than 100, but this parameter can be set on the command line. The above command compute term statistics for a segment of ClueWeb12. It's easier to compute term statistics segment by segment to generate smaller and more manageable Hadoop jobs.

Compute term statistics for all the other segments in the same manner.

Next, merge all the segment statistics together:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.app.MergeTermStatistics \
 -input term-stats/segment* -output term-stats-all
```

Finally, build the dictionary:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.app.BuildDictionary \
 -input term-stats-all -output dictionary -count 7160086
```

You need to provide the number of terms in the dictionary via the `-count` option. That value is simply the number of output reducers from `MergeTermStatistics`.

To explore the contents of the dictionary, use this little interactive program:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.dictionary.DefaultFrequencySortedDictionary dictionary
```

On ClueWeb12, following the above instructions will create a dictionary with 7,160,086 terms.


**Implementation details:** Tokenization is performed by first using Jsoup throw away all markup information and then passing the resulting text through Lucene's `StandardAnalyzer`.

The dictionary has two components: the terms are stored as a front-coded list (which necessarily means that the terms must be sorted); a monotone minimal perfect hash function is used to hash terms (strings) into the lexicographic position. Term to termid lookup is accomplished by the hashing function (to avoid binary searching through the front-coded data structure, which is expensive). Termid to term lookup is accomplished by direct accesses into the front-coded list. An additional mapping table is used to convert the lexicographic position into the (*df*-sorted) termid. 

Building Document Vectors
-------------------------

With the dictionary, we can now convert the entire collection into a sequence of document vectors, where each document vector is represented by a sequence of termids; the termids map to the sequence of terms that comprise the document. These document vectors are much more compact and much faster to scan for processing purposes.

To build document vectors, issue the following command:

```
hadoop jar target/clueweb-tools-0.1-SNAPSHOT.jar \
 org.clueweb.clueweb12.app.BuildDocVectors \
 -input /data/private/clueweb12/Disk1/ClueWeb12_00/*/*.warc.gz \
 -output /data/private/clueweb12/derived/docvectors.20130710/segment00 \
 -dictionary /data/private/clueweb12/derived/dictionary.20130710 \
 -reducers 100
```

Once again, it's advisable to run on a segment at a time in order to keep the Hadoop job sizes manageable. Note that the program run identity reducers to repartition the document vectors into 100 parts (to avoid the small files problem).

The output directory will contain `SequenceFile`s, with a `Text` containing the WARC-TREC-ID as the key, and a `BytesWritable` object as the value. The value contains a stream of integers written using Hadoop's VInt util (for writing variable-length integers).

To read back these document vectors, check out `org.clueweb.clueweb12.app.ProcessDocVectors` for some demo code.

The entire ClueWeb12 collection, converted into document vectors, occupies roughly 1.08 TB (compared to 5.54 TB compressed in the original WARC files).

License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
