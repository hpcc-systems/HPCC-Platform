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


package TestSOAP::TestService;

use strict;
use warnings;

sub _wrap(@)
{
    return SOAP::Data->name(@_);
}

sub greeting($$)
{
    my ($class, $name) = @_;
    $name =~ s/[[:space:]]+$//;
    return _wrap(salutation => "hello $name");
}

sub espsplit($$)
{
    my ($class, $list) = @_;
    my @items = split(/[[:space:]]+/, $list);
    my @results = map(_wrap(Row => [_wrap(item => $_)]), @items);
    return _wrap(Results => [_wrap(Result => [_wrap(Dataset => \@results)])]);
}

1;
