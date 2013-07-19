use strict;
use warnings;

#this program takes two TREC result files as input and a maximum rank and computes for each query the overlap between the two result files.


my $numArgs=$#ARGV+1;
if($numArgs!=3)
{
	print "Usage: [trec format run 1] [trec format run 2] [comparison up to rank]\n";
	exit
}

#201 Q0 clueweb12-0905wb-50-14578 1 -6.0206194 lmretrieval

my $maxRank = $ARGV[2];

my %res1=();

open(IN,$ARGV[0])||die $!;
while(<IN>)
{
	chomp;
	my @tok = split(/\s+/,$_);
	my $qid = $tok[0];
	my $did = $tok[2];
	my $rank= $tok[3];
	if($rank>$maxRank){next;}

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

open(IN,$ARGV[1])||die $!;
while(<IN>)
{
	chomp;
	my @tok = split(/\s+/,$_);
	my $qid = $tok[0];
	my $did = $tok[2];
	my $rank= $tok[3];
	if($rank>$maxRank){next;}

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
