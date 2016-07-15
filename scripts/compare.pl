use strict;
use warnings;

#This script takes as input two files in TREC result file format and a maximum rank.
#It proceeds to read the docids of each query in each file up to the maximum rank.
#Then, a query-wise overlap in docids is computed.
#Note that the maximum rank is simply the number of docids read per query, it is NOT
#the rank number present in the TREC result file (as the spam filtering results
#would not be comparable for instance).

my $numArgs = $#ARGV+1;

if($numArgs!=3)
{
	print "Usage: [trec result file 1] [trec result file 2] [max rank]\n";
	exit;
}

my $maxRank = $ARGV[2];

my %res1=();

open(IN,$ARGV[0])||die $!;
my $rank = 1;
my $currentQuery = "-1";
while(<IN>)
{
	chomp;
	my @tok = split(/\s+/,$_);
	my $qid = $tok[0];
	my $did = $tok[2];

	if($qid ne $currentQuery){$currentQuery=$qid; $rank=1;}

	if($rank>$maxRank){next;}

	$rank++;
	$res1{$qid."_".$did}=1;
}
close(IN);

my $num=0;
my $overlap=0;

my %nums=();
my %overlaps=();


foreach my $key(keys %res1)
{
	my $qid = $key;
	$qid=~s/_.*//;
	$nums{$qid}=0;
	$overlaps{$qid}=0;
}

$rank=1;
$currentQuery="-1";
open(IN,$ARGV[1])||die $!;
while(<IN>)
{
	chomp;
	my @tok = split(/\s+/,$_);
	my $qid = $tok[0];
	my $did = $tok[2];

	if($qid ne $currentQuery){$currentQuery=$qid; $rank=1;}

	if($rank>$maxRank){next;}

	$rank++;
	if(exists $nums{$qid}){;}
	else{next;}
	$nums{$qid}=1 + $nums{$qid};
	
	$num++;
	if(exists $res1{$qid."_".$did})
	{
		$overlap++;
		$overlaps{$qid}=$overlaps{$qid}+1;
	}
}
close(IN);

my $percentage = 100 * $overlap / $num;

print "Percentage of overlap up to rank $maxRank: $percentage\n";

foreach my $qid( sort {$a<=>$b} keys %overlaps)
{
	my $p = 100 * $overlaps{$qid}/$nums{$qid};
	print "Query $qid => overlap: $p%\n";
}
