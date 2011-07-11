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

package Regress::NormalizeFP;

=pod

=head1 NAME

Regress::NormalizeFP - perl module used by runregress to normalize floating point values

=head1 SYNOPSIS

my $filter = Regress::NormalizeFP->new(" 5\n");

my $output = $filter->apply("abc -0.001 1.23123123 -123123123123123123123 xyz");

=cut

use strict;
use warnings;
use Exporter;
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item my $filter = Regress::NormalizeFP->new($dp);

Return a filter which rounds everything to $dp decimal places, and anything which is zero to $dp decimal places to 0 (to avoid the -0 problem).

=cut

sub new($$)
{
    my ($class, $dp) = @_;
    my $self = {zero => 10**-$dp,
                format => '%.' . $dp . 'g'};
    return bless($self, $class);
}

=pod

=item my $output = $filter->apply($ref);

Applies the filter to the scalar referred to by the argument, normalizing any floating point values it finds.

=cut

sub apply($$)
{
    my ($self, $ref) = @_;
    $$ref =~ s/([+-]?\d+\.\d+([eE][+-]?\d+)?)/$self->_normalize($1)/eg;
}

#private

sub _normalize($$)
{
    my ($self, $val) = @_;
    return 0 if(($val < $self->{zero}) && ($val > -$self->{zero}));
    return sprintf($self->{format}, $val);
}

=pod

=back

=cut

1;
