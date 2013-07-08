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

Note that `appassembler:assemble` automatically generates a few launch scripts for you.

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


License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
