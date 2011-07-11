#! /usr/bin/perl
################################################################################
#
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
use SOAP::Transport::HTTP qw();

my ($logfile, $libpath, $maxsleep);

BEGIN
{
    my $cfgfile = 'soapserver.ini';
    my $cfg = new Config::Simple();
    $cfg->read($cfgfile) or die("Could not read config file $cfgfile: $!");
    $logfile = $cfg->param('logfile') or die("No logfile in config file $cfgfile");
    $libpath = $cfg->param('libpath') or die("No libpath in config file $cfgfile");
    $maxsleep = $cfg->param('maxsleep') || 0;
}
use lib ($libpath);

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

open(OUT, '>>', $logfile) or die("Could not write to $logfile: $!");
print(OUT "Request at $time\n");
print(OUT "[$_]=[$ENV{$_}]\n") foreach (sort(keys(%ENV)));
print(OUT "[current directory]=[", cwd(), "]\n");
if($maxsleep)
{
    my $sec = rand($maxsleep)+1;
    print(OUT "Sleeping for $sec seconds\n");
    sleep($sec);
}
print(OUT "Dispatching\n");

my $soap = SOAP::Transport::HTTP::CGI->dispatch_to('TestSOAP::TestService');
$soap->on_dispatch(\&my_on_dispatch);
eval { $soap->handle(); };
if($@)
{
    print(OUT "Transport error: $@\n\n");
}
else
{
    my $resp = $soap->response();
    print(OUT "Response status: ", $resp->status_line(), "\n");
    print(OUT "Response content: ", $resp->decoded_content(), "\n");    
    print(OUT "OK\n\n");
}

close(OUT);

sub my_on_dispatch($)
{
    my ($request) = @_;
    $request->match((ref($request))->method());
    my ($method_uri, $method_name) = ($request->namespaceuriof() || '', $request->dataof()->name());
    $method_name =~ s/Request$//;
    return ($method_uri, $method_name);
}
