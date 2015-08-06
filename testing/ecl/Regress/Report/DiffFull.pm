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

package Regress::Report::DiffFull;

=pod

=head1 NAME
    
Regress::Report::DiffFull - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Regress::Report::Diff qw();
use Exporter;
our @ISA = qw(Regress::Report::Diff);

#PUBLIC

sub describe($)
{
    my ($type) = @_;
    return 'As Diff, but also writes lists of results which matched, were not compared (because no key, as in a setup run), or skipped.';
}

#PROTECTED

sub _start_matches($)
{
    my ($self) = @_;
    print("Matches:\n");
}

sub _match($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_matches($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_nocmp($)
{
    my ($self) = @_;
    print("Completed without error:\n");
}

sub _nocmp($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    print($variant ? "    $query / $variant\n" : "    $query\n");
}

sub _end_nocmp($)
{
    my ($self) = @_;
    print("\n");
}

sub _start_skipped($)
{
    my ($self) = @_;
    print("Skipped:\n");
}

sub _skipped($$$)
{
    my ($self, $query, $variant, $reason) = @_;
    print($variant ? "    $query / $variant : $reason\n" : "    $query : $reason\n");
}

sub _end_skipped($)
{
    my ($self) = @_;
    print("\n");
}

=pod

=head1 DESCRIPTION

This report type subclasses L<Regress::Report::Diff> and extends it to also write lists of results which matched, were not compared (because no key, as in a setup run), or were skipped.

=cut

1;
