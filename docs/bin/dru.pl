#!perl
###########################################################
#    dru.pl                                               #
#   usage: run in directory of html files for drupal      #
###########################################################
#  GP 2011 
#  This program cleans html files bound for drupal        #
#  of codes that will cause drupal errors                 #
#  This program loops through all html files in a dir     #
#  then will substitute the strings on line 37&38 s//;    #
#  added logic to further drupalize the LINK tags s//;    #
#  finally it will write out the modified file 1 pass     #
###########################################################
#  Copyright LN 2011 : all rights reserved       #
###########################################################


use File::Find;
use strict;

my $directory = ".";

find (\&process, $directory);

sub process
{
    my @outLines;  #Data we are going to output
    my $line;      #Data we are reading line by line

    #  print "processing $_ / $File::Find::name\n";

    # Only parse files that end in .html
    if ( $File::Find::name =~ /\.html$/ ) {

        open (FILE, $File::Find::name ) or
        die "Cannot open file: $!";

        print "\n" . $File::Find::name . "\n";
        while ( $line = <FILE> ) {
        $line =~ s~<a name\="([^"]*)"><\/a>~~gi;
        $line =~ s~<a class\="indexterm" name\="d0e.[^"]*"><\/a>~~gi;
#following line added for see also links for ECLR
        $line =~ s/<a class\="link" href\="(.[^\.]*)\.html"/<a class\="link" href\="$1"/g;  
##following lines colorize text 
         $line =~ s~<span class\="bluebold">~<span class\="bluebold" style\="color:blue\;font-weight:bold">~gi;
         $line =~ s~<span class\="blueital">~<span class\="blueital" style\="color:blue\;font-style:italic">~gi;
         $line =~ s~<span class\="blue">~<span class\="blue" style\="color:blue">~gi;
         $line =~ s~<span class\="redbold">~<span class\="redbold" style\="color:red\;font-weight:bold">~gi;
         $line =~ s~<span class\="redital">~<span class\="redital" style\="color:red\;font-style:italic">~gi;
         $line =~ s~<span class\="red">~<span class\="red" style\="color:red">~gi;
         $line =~ s~<span class\="greenbold">~<span class\="greenbold" style\="color:green\;font-weight:bold">~gi;
         $line =~ s~<span class\="greenital">~<span class\="greenital" style\="color:green\;font-style:italic">~gi;
         $line =~ s~<span class\="green">~<span class\="green" style\="color:green">~gi;
         $line =~ s~<span class\="whitebold">~<span class\="whitebold" style\="color:white\;font-weight:bold">~gi;
	 $line =~ s~<span class\="whiteital">~<span class\="whiteital" style\="color:white\;font-style:italic">~gi;
         $line =~ s~<span class\="white">~<span class\="white" style\="color:white">~gi;
   
   print $line;
   push(@outLines, $line);
        }
        close FILE;

        open ( OUTFILE, ">$File::Find::name" ) or
        die "Cannot open file: $!";

        print ( OUTFILE @outLines );
        close ( OUTFILE );
       
        undef( @outLines );
    }
}