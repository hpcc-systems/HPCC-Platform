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

package Regress::Report::Summary;

=pod

=head1 NAME
    
Regress::Report::Summary - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Regress::Report::Basic qw();
use Exporter;
our @ISA = qw(Regress::Report::Basic);

#PUBLIC

sub precheck($)
{
    my ($self) = @_;
    return undef;
}

sub describe($)
{
    my ($type) = @_;
    return 'Writes a summary of the results to standard output. Includes numbers of results matching, failing (containing errors when the key did not), differing, skipped-as-TBD, not compared (because no key file, as in a setup run), not run, missing, unexpected, skipped, and excluded from the report (by the query option); and lists of results differing, not run, missing, and unexpected.';
}

#PROTECTED

sub _start($)
{
    my ($self) = @_;
}

sub _start_summary($)
{
    my ($self) = @_;
    print("Summary of results:\n");
}

sub _sum_matches($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'match', 'matches');
}

sub _sum_failures($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'failure', 'failures');
}

sub _sum_diffs($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'mismatch', 'mismatches');
}

sub _sum_tbd($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'skipped as TBD');
}

sub _sum_nocmp($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'completed without error');
}

sub _sum_notrun($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'query not run', 'queries not run');
}

sub _sum_missing($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'missing result file', 'missing result files');
}

sub _sum_unexpected($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'unexpected result file', 'unexpected result files');
}

sub _sum_skipped($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'skipped');
}

sub _sum_excluded($$)
{
    my ($self, $count) = @_;
    _print_count($count, 'excluded from report');
}

sub _end_summary($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_matches($)
{
    my ($self) = @_;
}

sub _match($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
}

sub _end_matches($)
{
    my ($self) = @_;
}

sub _start_failures($)
{
    my ($self) = @_;
    print("Failures:\n");
}

sub _failure($$$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_failures($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_diffs($)
{
    my ($self) = @_;
    print("Mismatches:\n");
}

sub _diff($$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_diffs($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_tbd($)
{
    my ($self) = @_;
    print("Skipped as TBD:\n");
}

sub _tbd($$$)
{
    my ($self, $query, $variant, $reason) = @_;
    print($variant ? "    $query / $variant : $reason\n" : "    $query : $reason\n");
}

sub _end_tbd($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_nocmp($)
{
    my ($self) = @_;
}

sub _nocmp($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
}

sub _end_nocmp($)
{
    my ($self) = @_;
}

sub _start_notrun($)
{
    my ($self) = @_;
    print("Queries not run:\n");
}

sub _notrun($$$)
{
    my ($self, $query, $variant) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_notrun($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_missing($)
{
    my ($self) = @_;
    print("Missing result files:\n");
}

sub _missing($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_missing($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_unexpected($)
{
    my ($self) = @_;
    print("Unexpected result files:\n");
}

sub _unexpected($$)
{
    my ($self, $outfile) = @_;
    print("    $outfile\n");
}

sub _end_unexpected($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_skipped($)
{
    my ($self) = @_;
}

sub _skipped($$$$)
{
    my ($self, $query, $variant, $reason) = @_;
}

sub _end_skipped($)
{
    my ($self) = @_;
}

sub _end($)
{
    my ($self) = @_;
}

#PRIVATE

sub _print_count($$;$)
{
    my ($count, $singular, $plural) = @_;
    my $label = ($plural && ($count != 1)) ? $plural : $singular;
    print("    $count $label\n");
}

=pod

=head1 DESCRIPTION

This report type writes a summary of the results to standard output. It includes numbers of results matching, failing (containing errors when the key did not), differing, skipped-as-TBD, not compared (because no key file, as in a setup run), not run, missing, unexpected, and skipped; and lists of results differing, skipped-as-TBD, not run, missing, and unexpected.

=cut

1;
