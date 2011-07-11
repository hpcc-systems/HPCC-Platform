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

package Regress::Report::BC2;

=pod

=head1 NAME
    
Regress::Report::BC2 - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use File::Spec::Functions qw(catdir catfile splitpath);
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
    my @bc2args;
    push(@bc2args, catdir($self->{suite}, 'key'));
    push(@bc2args, catdir($self->{suite}, 'out'));
    push(@bc2args, '/readonly');
    push(@bc2args, '/filters=' . join(';', $self->_outfile_list())) if($self->{report_queries} || $self->{variant});
    $self->{engine}->launch('bc2', \@bc2args);
}

sub describe($)
{
    my ($type) = @_;
    return 'Launches Beyond Compare on the output and key directories.';
}

#PRIVATE

sub _outfile_list($)
{
    my ($self) = @_;
    my @outfiles;
    my $manifestfile = catfile($self->{suite}, 'manifest.csv');
    open(MANIFEST, '<', $manifestfile) or $self->{engine}->error("Could not read $manifestfile: $!");
    while(<MANIFEST>)
    {
        chomp();
        my ($query, $variant, $keypath, $outpath) = split(/,/);
        next if($self->_filtered($query, $variant));
        my (undef, undef, $outfile) = splitpath($outpath);
        push(@outfiles, $outfile);
    }
    close(MANIFEST);
    $self->{engine}->error("All queries excluded from report by filters") unless(@outfiles);
    return @outfiles;
}

sub _filtered($$$)
{
    my ($self, $query, $variant) = @_;
    return 1 if($self->{report_queries} && !grep({$query eq $_} @{$self->{report_queries}}));
    return 1 if($self->{variant} && !grep({$variant eq $_} @{$self->{variant}}));
    return 0;
}

=pod

=head1 DESCRIPTION

This report type launches Beyond Compare on the output and key directories.

=cut

1;

