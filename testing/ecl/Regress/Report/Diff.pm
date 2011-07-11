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

package Regress::Report::Diff;

=pod

=head1 NAME
    
Regress::Report::Diff - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Text::Diff qw(diff);
use Regress::Report::Summary qw();
use Exporter;
our @ISA = qw(Regress::Report::Summary);

#PUBLIC

sub describe($)
{
    my ($type) = @_;
    return 'Writes a list of mismatches of the results to standard output. Includes numbers of results matching, failing (containing errors when the key did not), differing, skipped-as-TBD, not compared (because no key file, as in a setup run), not run, missing, unexpected, skipped, and excluded from the report (by the query option); full text of failing results; full diffs in a standard form for results differing; and lists of results skipped-as-TBD, not run, missing, and unexpected.';
}

#PROTECTED

sub _failure($$$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    print("    $query");
    print(" / $variant") if($variant);
    print(" ($wuid)") if($wuid);
    print("\n\n");
    open(ERRORIN, '<', $outpath) or $self->error("Could not read $outpath: $!");
    print while(<ERRORIN>);
    close(ERRORIN);
    print("\n");
}

sub _diff($$$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    print("    $query");
    print(" / $variant") if($variant);
    print(" ($wuid)") if($wuid);
    print("\n\n");
    diff($keypath, $outpath, {OUTPUT => \*STDOUT});
    print("\n");
}

=pod

=head1 DESCRIPTION

This report type writes a list of mismatches of the results to standard output. It includes numbers of results matching, failing (containing errors when the key did not), differing, skipped-as-TBD, not compared (because no key file, as in a setup run), not run, missing, unexpected, and skipped; full text (and WUID where available) of failing results; full diffs in a standard form (and WUID where available) for results differing; and lists of results skipped-as-TBD, not run, missing, and unexpected.

=cut

1;
