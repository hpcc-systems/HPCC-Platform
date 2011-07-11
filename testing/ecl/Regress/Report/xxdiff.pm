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

package Regress::Report::xxdiff;

=pod

=head1 NAME
    
Regress::Report::xxdiff - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use File::Spec::Functions qw(catdir);
use Regress::Report qw();
use Exporter;
our @ISA = qw(Regress::Report);

#PUBLIC

sub precheck($)
{
    my ($self) = @_;
    return undef;
}

sub generate($)
{
    my ($self) = @_;
    $self->{engine}->warning("xxdiff report type does support the filtering of results using the query option (because xxdiff does not easily do that)") if($self->{query} && $self->{rundesc}->{query});
    $self->{engine}->warning("xxdiff report type does support the filtering of results using the variant option (because xxdiff does not easily do that)") if($self->{variant} && $self->{rundesc}->{variant});
    my $keydir = catdir($self->{suite}, 'key');
    my $outdir = catdir($self->{suite}, 'out');
    $self->{engine}->launch('xxdiff', [$keydir, $outdir]);
}

sub describe($)
{
    my ($type) = @_;
    return 'Launches xxdiff on the output and key directories.';
}

=pod

=head1 DESCRIPTION

This report type launches xxdiff on the output and key directories.

=cut

1;

