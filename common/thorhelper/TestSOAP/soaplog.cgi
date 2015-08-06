#! /usr/bin/perl

################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems(R).
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#################################################################################


use strict;
use warnings;
use Cwd qw(cwd);
use Config::Simple qw();

my $logfile;

BEGIN
{
    my $cfgfile = 'soapserver.ini';
    my $cfg = new Config::Simple();
    $cfg->read($cfgfile) or die("Could not read config file $cfgfile: $!");
    $logfile = $cfg->param('logfile') or die("No logfile in config file $cfgfile");
}

my $time = localtime();
my $method = $ENV{REQUEST_METHOD} || 'NONE';
unless($method eq 'POST')
{
    print("Content-type: text/plain\n\n");
    print("Expected POST request, received $method\n");
    open(OUT, '>>', $logfile) or die("could not write to $logfile: $!");
    print(OUT "Bad request at $time: method was $method\n");
    close(OUT);
    exit(1);
}

open(OUT, '>>', $logfile) or die("could not write to $logfile: $!");
print(OUT "Request at $time\n");
print(OUT "[$_]=[$ENV{$_}]\n") foreach (sort(keys(%ENV)));
print(OUT "[current directory]=[", cwd(), "]\n");
print(OUT) while(<>);
print(OUT "\n.\n\n");
close(OUT);

print("Content-type: text/xml\n\n");
print("<ok/>\n");
