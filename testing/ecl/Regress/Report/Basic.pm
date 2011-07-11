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

package Regress::Report::Basic;

=pod

=head1 NAME
    
Regress::Report::Basic - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Regress::Report qw();
use File::Spec::Functions qw(catfile);
use File::Compare qw();
use Exporter;
our @ISA = qw(Regress::Report);

#PUBLIC

sub generate($)
{
    my ($self) = @_;
    $self->_analyse();
    $self->_start();

    $self->_start_summary();
    if($self->{matches})
    {
        my $count = @{$self->{matches}};
        $self->_sum_matches($count);
    }
    if($self->{failures})
    {
        my $count = @{$self->{failures}};
        $self->_sum_failures($count);
    }
    if($self->{diffs})
    {
        my $count = @{$self->{diffs}};
        $self->_sum_diffs($count);
    }
    if($self->{tbd})
    {
        my $count = @{$self->{tbd}};
        $self->_sum_tbd($count);
    }
    if($self->{nocmp})
    {
        my $count = @{$self->{nocmp}};
        $self->_sum_nocmp($count);
    }
    if($self->{notrun})
    {
        my $count = @{$self->{notrun}};
        $self->_sum_notrun($count);
    }
    if($self->{missing})
    {
        my $count = @{$self->{missing}};
        $self->_sum_missing($count);
    }
    if($self->{unexpected})
    {
        my $count = @{$self->{unexpected}};
        $self->_sum_unexpected($count);
    }
    if($self->{skipped})
    {
        my $count = @{$self->{skipped}};
        $self->_sum_skipped($count);
    }
    if($self->{excluded})
    {
        my $count = @{$self->{excluded}};
        $self->_sum_excluded($count);
    }
    $self->_end_summary();

    if($self->{matches})
    {
        $self->_start_matches();
        $self->_match($_->{query}, $_->{variant}, $_->{wuid}) foreach (@{$self->{matches}});
        $self->_end_matches();
    }
    if($self->{failures})
    {
        $self->_start_failures();
        $self->_failure($_->{query}, $_->{variant}, $_->{keypath}, $_->{outpath}, $_->{wuid}) foreach (@{$self->{failures}});
        $self->_end_failures();
    }
    if($self->{diffs})
    {
        $self->_start_diffs();
        $self->_diff($_->{query}, $_->{variant}, $_->{keypath}, $_->{outpath}, $_->{wuid}) foreach (@{$self->{diffs}});
        $self->_end_diffs();
    }
    if($self->{tbd})
    {
        $self->_start_tbd();
        $self->_tbd($_->{query}, $_->{variant}, $_->{reason}) foreach (@{$self->{tbd}});
        $self->_end_tbd();
    }
    if($self->{nocmp})
    {
        $self->_start_nocmp();
        $self->_nocmp($_->{query}, $_->{variant}, $_->{wuid}) foreach (@{$self->{nocmp}});
        $self->_end_nocmp();
    }
    if($self->{notrun})
    {
        $self->_start_notrun();
        $self->_notrun($_->{query}, $_->{variant}) foreach (@{$self->{notrun}});
        $self->_end_notrun();
    }
    if($self->{missing})
    {
        $self->_start_missing();
        $self->_missing($_->{query}, $_->{variant}, $_->{wuid}) foreach (@{$self->{missing}});
        $self->_end_missing();
    }
    if($self->{unexpected})
    {
        $self->_start_unexpected();
        $self->_unexpected($_->{outpath}) foreach (@{$self->{unexpected}});
        $self->_end_unexpected();
    }
    if($self->{skipped})
    {
        $self->_start_skipped();
        $self->_skipped($_->{query}, $_->{variant}, $_->{reason}) foreach (@{$self->{skipped}});
        $self->_end_skipped();
    }

    $self->_end();
}

sub describe($)
{
    my ($type) = @_;
    return undef;
}

#PROTECTED

sub _start($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_summary($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_matches($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_diffs($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_failures($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_tbd($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_nocmp($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_missing($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_unexpected($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_skipped($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _sum_excluded($$)
{
    my ($self, $count) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_summary($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_matches($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _match($$$)
{
    my ($self, $query, $variant) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_matches($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_failures($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _failure($$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_failures($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_diffs($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _diff($$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_diffs($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_tbd($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _tbd($$$$)
{
    my ($self, $query, $variant, $reason) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_tbd($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_nocmp($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _nocmp($$$)
{
    my ($self, $query, $variant) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_nocmp($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_missing($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _missing($$$)
{
    my ($self, $query, $variant) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_missing($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_unexpected($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _unexpected($$)
{
    my ($self, $outfile) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_unexpected($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _start_skipped($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _skipped($$$$)
{
    my ($self, $query, $variant, $reason) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end_skipped($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

sub _end($)
{
    my ($self) = @_;
    $self->{engine}->error("Unimplemented method in report type $self->{typename}");
}

#PRIVATE

sub _analyse($)
{
    my ($self) = @_;
    $self->_read_wuids();
    $self->_read_manifest();
    $self->_read_skipped('skipped');
    $self->_read_skipped('tbd');
    $self->_find_unexpected();
}

sub _read_wuids($)
{
    my ($self) = @_;
    my $wuidsfile = catfile($self->{suite}, 'wuids.csv');
    unless(-e $wuidsfile)
    {
        $self->{engine}->warning("Suite was apparently never run (no wuids.csv)");
        return;
    }
    open(WUIDS, '<', $wuidsfile) or $self->{engine}->error("Could not read $wuidsfile: $!");
    while(<WUIDS>)
    {
        chomp();
        $self->_set_wuid(split(/,/));
    }
    close(WUIDS);
}

sub _read_manifest($)
{
    my ($self) = @_;
    my $manifestfile = catfile($self->{suite}, 'manifest.csv');
    open(MANIFEST, '<', $manifestfile) or $self->{engine}->error("Could not read $manifestfile: $!");
    while(<MANIFEST>)
    {
        chomp();
        my ($query, $variant, $keypath, $outpath) = split(/,/);
        $self->_analyse_query($query, $variant, $keypath, $outpath);
        $self->{expected}->{$outpath} = 1;
    }
    close(MANIFEST);
}

sub _read_skipped($$)
{
    my ($self, $type) = @_;
    my $skippedfile = catfile($self->{suite}, "$type.csv");
    open(SKIPPED, '<', $skippedfile) or $self->{engine}->error("Could not read $skippedfile: $!");
    while(<SKIPPED>)
    {
        chomp();
        my ($query, $variant, $reason) = split(/,/);
        if($self->_filtered($query, $variant))
        {
            push(@{$self->{excluded}}, {query => $query, variant => $variant});
        }
        else
        {
            push(@{$self->{$type}}, {query => $query, variant => $variant, reason => $reason});
        }
    }
    close(SKIPPED);
}

sub _find_unexpected($)
{
    my ($self) = @_;
    my $outpattern = catfile($self->{suite}, 'out', '*.xml');
    foreach my $outpath (glob($outpattern))
    {
        push(@{$self->{unexpected}}, {outpath => $outpath}) unless($self->{expected}->{$outpath});
    }
}

sub _analyse_query($$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath) = @_;
    if($self->_filtered($query, $variant))
    {
        push(@{$self->{excluded}}, {query => $query, variant => $variant});
        return;
    }
    my $params = {query => $query, variant => $variant, outpath => $outpath};
    my $wuid = $self->_get_wuid($query, $variant);
    $params->{wuid} = $wuid if($wuid && ($wuid ne '__NONE__'));
    if(-f $outpath)
    {
        if($keypath)
        {
            $params->{keypath} = $keypath;
            $self->{engine}->error("Missing key file $keypath") unless(-f $keypath);
            my $cmp = File::Compare::compare($keypath, $outpath);
            $self->{engine}->error("Error comparing $keypath and $outpath: $!") if($cmp == -1);
            if($cmp)
            {
                if($self->_has_errors($outpath) && !$self->_has_errors($keypath))
                {
                    push(@{$self->{failures}}, $params);
                }
                else
                {
                    push(@{$self->{diffs}}, $params);
                }
            }
            else
            {
                push(@{$self->{matches}}, $params);
            }
        }
        else
        {
            if($self->_has_errors($outpath))
            {
                push(@{$self->{failures}}, $params);
            }
            else
            {
                push(@{$self->{nocmp}}, $params);
            }
        }
    }
    else
    {
        if($wuid)
        {
            push(@{$self->{missing}}, $params);
        }
        else
        {
            push(@{$self->{notrun}}, $params);
        }
    }
}

sub _set_wuid($$$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    $wuid = '__NONE__' unless($wuid);
    if($variant)
    {
        $self->{vari_wuids}->{$query}->{$variant} = $wuid;
    }
    else
    {
        $self->{sing_wuids}->{$query} = $wuid;
    }
}

sub _get_wuid($$$)
{
    my ($self, $query, $variant) = @_;
    my $wuid = ($variant ? $self->{vari_wuids}->{$query}->{$variant} : $self->{sing_wuids}->{$query});
    return $wuid;
}

sub _has_errors($$)
{
    my ($self, $path) = @_;
    open(ERRORIN, '<', $path) or $self->error("Could not read $path: $!");
    while(<ERRORIN>)
    {
        return 1 if(/^error:/i);
        return 1 if(/^<error/i);
        return 1 if(/^<exception/i);
    }
    close(ERRORIN);
    return 0;
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

This class subclasses and partially implements L<Regress::Report>, and is intended to be further subclassed. It implements L<generates> to run through the suite and identify results which match, which differ, which were not compared (because no key file, as in a setup run), which were not run (perhaps because the script was interupted), which are missing, which are unexpected, and which have been skipped (including which are TBD). It then calls the following methods in turn. These methods must be implemented in the subclass.

=over

=item *

$self->_start();

=item *

$self->_start_summary();

=item *

$self->_sum_matches($count); # if any matches

=item *

$self->_sum_failures($count); # if any failures

=item *

$self->_sum_diffs($count); # if any mismatches

=item *

$self->_sum_tbd($count); # if any TBD

=item *

$self->_sum_nocmp($count); # if any not compared

=item *

$self->_sum_notrun($count); # if any not run

=item *

$self->_sum_missing($count); # if any missing

=item *

$self->_sum_unexpected($count); # if any unexpected

=item *

$self->_sum_skipped($count); # if any skipped

=item *

$self->_sum_excluded($count); # if any excluded from report with query option

=item *

$self->_end_summary();

=item *

$self->_start_matches(); # if any matches

=item *

$self->_match($query, $variant, $wuid); # for each match

=item *

$self->_end_matches(); # if any matches

=item *

$self->_start_failures(); # if any failures

=item *

$self->_failure($query, $variant, $keypath, $outpath, $wuid); # for each failure

=item *

$self->_end_failures(); # if any failures

=item *

$self->_start_diffs(); # if any mismatches

=item *

$self->_diff($query, $variant, $keypath, $outpath, $wuid); # for each mismatch

=item *

$self->_end_diffs(); # if any mismatches

=item *

$self->_start_tbd(); # if any skipped-as-TBD

=item *

$self->_tbd($query, $variant, $reason); # for each skipped-as-TBD

=item *

$self->_end_tbd(); # if any skipped-as-TBD

=item *

$self->_start_nocmp(); # if any not compare

=item *

$self->_nocmp($query, $variant, $wuid); # for each not compared

=item *

$self->_end_nocmp(); # if any not compared

=item *

$self->_start_notrun(); # if any not run

=item *

$self->_notrun($query}, $variant); # for each not run

=item *

$self->_end_notrun(); # if any not run

=item *

$self->_start_missing(); # if any missing

=item *

$self->_missing($query}, $variant, $wuid); # for each missing

=item *

$self->_end_missing(); # if any missing

=item *

$self->_start_unexpected(); # if any unexpected

=item *

$self->_unexpected($outfile); # for each unexpected

=item *

$self->_end_unexpected(); # if any unexpected

=item *

$self->_start_skipped(); # if any skipped

=item *

$self->_skipped($query, $variant, $reason); # for each skipped

=item *

$self->_end_skipped(); # if any skipped

=item *

$self->_end();

=back

=cut

1;
