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

