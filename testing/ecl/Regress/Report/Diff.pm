################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems(R).
#
#    This program is free software: you can redistribute it and/or modify
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
