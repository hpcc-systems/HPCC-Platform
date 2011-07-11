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
