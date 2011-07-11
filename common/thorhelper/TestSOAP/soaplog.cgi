#! /usr/bin/perl

################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
