################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or modify
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
################################################################################

package Regress::Report::Default;

=pod

=head1 NAME
    
Regress::Report::Default - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Regress::Report::Summary qw();
use Exporter;
our @ISA = qw(Regress::Report::Summary);

#PUBLIC

sub describe($)
{
    my ($type) = @_;
    return undef;
}

#PROTECTED

sub _end($)
{
    my ($self) = @_;
    print("To generate a full report:\n    use the report option or configuration variable.\n");
    print("To generate a report on these results without rerunning the suite:\n    use the norun option.\n");
    print("To see a list of available report types:\n    use the listreports option.\n");
    print("\n");
}

=pod

=head1 DESCRIPTION

This report type is intended for use as the default. It writes a summary of the results to standard output, followed by a message telling the user how to generate other reports. The summary is identical to that of L<Regress::Report::Summary>.

=cut

1;
