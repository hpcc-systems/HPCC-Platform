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
