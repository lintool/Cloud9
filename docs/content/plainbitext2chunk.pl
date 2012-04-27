#!/usr/bin/perl

# usage: plainbitext2chunk.pl [F-file] [E-file] [corpus-name] [F-langcode] [E-langcode]

open(FILE,$ARGV[0]) || die;
open(E,$ARGV[1])|| die;
@ftext = <FILE>;
#@etext = <E>;

$corpusname = $ARGV[2];
$elang = $ARGV[4];
$flang = $ARGV[3];

$s = "<?xml version=\"1.0\" encoding=\"UTF8\"?>\n";
$s .= "<pdoc name=\"corpus.en\">\n";
print STDOUT $s;

$cnt=0;
while(<E>){
	chomp $_;
	$esent = $_;
	if($cnt%10000==0)  
	{print STDERR "$cnt...";}
	print STDOUT "<pchunk name=\"". $corpusname . "_" . ($cnt+1) ."\">\n";
	$fsent = $ftext[$cnt];
	chomp $fsent;
	print STDOUT "<s lang=\"".$elang."\">".$esent."</s>\n";
	print STDOUT "<s lang=\"".$flang."\">".$fsent."</s>\n";
	print STDOUT "</pchunk>\n";
	$cnt++;
}
print STDOUT "</pdoc>\n";
close(E);
close(FILE);
