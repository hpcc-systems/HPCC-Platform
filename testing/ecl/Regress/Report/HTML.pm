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

package Regress::Report::HTML;

=pod

=head1 NAME
    
Regress::Report::HTML - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use File::Spec::Functions qw(catfile);
use Template qw();
use Text::Diff qw(diff);
use Regress::Util::DiffHTML qw();
use Regress::Report::Basic qw();
use Exporter;
our @ISA = qw(Regress::Report::Basic);

#PUBLIC

sub precheck($)
{
    my ($self) = @_;
    $self->{templatefile} = $self->_find_file('html.tt') or return("Could not locate template");
    $self->{vars}->{stylesheetfile} = $self->_find_file('default.css') or return("Could not locate stylesheet");
    $self->{vars}->{wuidurl} = _expand_wuidurl($self->{server}) if($self->{server});
    return undef;
}

sub describe($)
{
    my ($type) = @_;
    return 'Writes an HTML report on the results to report.html in the suite directory.';
}

#PROTECTED

sub _start($)
{
    my ($self) = @_;
    my $ttopts = {PRE_CHOMP => 1};
    $self->{tt} = Template->new($ttopts) or $self->{engine}->error("Could not initiate HTML template: " . Template->error());
    $self->{vars}->{name} = $self->{configuration};
    $self->{vars}->{settings} = $self->{desc};
    $self->{vars}->{runsettings} = $self->{rundesc} if($self->{rundesc});
    $self->{diff} = Regress::Util::DiffHTML->new();
}

sub _start_summary($)
{
    my ($self) = @_;
}

sub _sum_matches($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'match', 'match', 'matches');
}

sub _sum_failures($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'fail', 'failure', 'failures');
}

sub _sum_diffs($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'mismatch', 'mismatch', 'mismatches');
}

sub _sum_tbd($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'tbd', 'skipped as TBD', 'skipped as TBD');
}

sub _sum_nocmp($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'noccmp', 'completed without error');
}

sub _sum_notrun($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'notrun', 'query not run', 'queries not run');
}

sub _sum_missing($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'missing', 'missing result file', 'missing result files');
}

sub _sum_unexpected($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'unexpected', 'unexpected result file', 'unexpected result files');
}

sub _sum_skipped($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, 'skipped', 'skipped', 'skipped');
}

sub _sum_excluded($$)
{
    my ($self, $count) = @_;
    $self->_add_count($count, undef, 'excluded from report', 'excluded from report');
}

sub _end_summary($)
{
    my ($self) = @_;
}

sub _start_matches($)
{
    my ($self) = @_;
}

sub _match($$$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    push(@{$self->{vars}->{matches}}, {query => $query, variant => $variant, wuid => $wuid});
}

sub _end_matches($)
{
    my ($self) = @_;
}

sub _start_failures($)
{
    my ($self) = @_;
}

sub _failure($$$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    my $text;
    open(QIN, '<', $outpath) or $self->{engine}->error("Could not read $outpath: $!");
    $text .= $_ while(<QIN>);
    close(QIN);
    push(@{$self->{vars}->{failures}}, {query => $query, variant => $variant, text => $text, wuid => $wuid});
}

sub _end_failures($)
{
    my ($self) = @_;
}

sub _start_diffs($)
{
    my ($self) = @_;
}

sub _diff($$$$$$)
{
    my ($self, $query, $variant, $keypath, $outpath, $wuid) = @_;
    my $diff = diff($keypath, $outpath, {STYLE => $self->{diff}});
    push(@{$self->{vars}->{mismatches}}, {query => $query, variant => $variant, diff => $diff, wuid => $wuid});
}

sub _end_diffs($)
{
    my ($self) = @_;
}

sub _start_tbd($)
{
    my ($self) = @_;
}

sub _tbd($$$$)
{
    my ($self, $query, $variant, $reason) = @_;
    push(@{$self->{vars}->{tbd}->{$reason}}, {query => $query, variant => $variant});
}

sub _end_tbd($)
{
    my ($self) = @_;
}

sub _start_nocmp($)
{
    my ($self) = @_;
}

sub _nocmp($$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    push(@{$self->{vars}->{nocmps}}, {query => $query, variant => $variant, wuid => $wuid});
}

sub _end_nocmp($)
{
    my ($self) = @_;
}

sub _start_notrun($)
{
    my ($self) = @_;
}

sub _notrun($$$)
{
    my ($self, $query, $variant) = @_;
    push(@{$self->{vars}->{notrun}}, {query => $query, variant => $variant});
}

sub _end_notrun($)
{
    my ($self) = @_;
}

sub _start_missing($)
{
    my ($self) = @_;
}

sub _missing($$$$)
{
    my ($self, $query, $variant, $wuid) = @_;
    push(@{$self->{vars}->{missing}}, {query => $query, variant => $variant, wuid => $wuid});
}

sub _end_missing($)
{
    my ($self) = @_;
}

sub _start_unexpected($)
{
    my ($self) = @_;
}

sub _unexpected($$)
{
    my ($self, $outfile) = @_;
    push(@{$self->{vars}->{unexpected}}, {outfile => $outfile});
}

sub _end_unexpected($)
{
    my ($self) = @_;
}

sub _start_skipped($)
{
    my ($self) = @_;
}

sub _skipped($$$$)
{
    my ($self, $query, $variant, $reason) = @_;
    push(@{$self->{vars}->{skipped}->{$reason}}, {query => $query, variant => $variant});
}

sub _end_skipped($)
{
    my ($self) = @_;
}

sub _end($)
{
    my ($self) = @_;
    my $outpath = catfile($self->{suite}, 'report.html');
    $self->{tt}->process($self->{templatefile}, $self->{vars}, $outpath) or $self->{engine}->error("Could not generate HTML using template: " . $self->{tt}->error());
}

#PRIVATE

sub _find_file($$)
{
    my ($self, $filename) = @_;
    foreach my $dir (@INC)
    {
        my $path = catfile($dir, 'Regress', 'Report', $filename);
        return $path if(-e $path);
    }
    my $path = catfile('Regress', 'Report', $filename);
    $self->{engine}->warning("Looking for $path on \@INC");
    my $incstr = join(';', @INC);
    $self->{engine}->warning("\@INC is $incstr");
    return undef;
}

sub _add_count($$$;$)
{
    my ($self, $count, $tag, $singular, $plural) = @_;
    my $label = ($plural && ($count != 1)) ?  $plural : $singular;
    push(@{$self->{vars}->{counts}}, {label => $label, tag => $tag, count => $count});
}

sub _expand_wuidurl($)
{
    my ($server) = @_;
    $server = "http://$server" unless($server =~ /^[[:alpha:]][[:alnum:]\+\-\.]*:/);
    $server = "$server:8010" unless($server =~ /:[[:digit:]]+$/);
    return "$server/?inner=../WsWorkunits/WUInfo%3FWuid%3D";
}

=pod

=head1 DESCRIPTION

Writes an HTML report on the results to report.html in the suite directory. Includes lists of all queries run, with failures and mismatches prominently displayed. Where WUIDs could be determined, contains hyperlinks to their ECL Watch pages. (The URL for ECL Watch is taken from the B<server> configuration variable: if there is no scheme prefix, C<http://> is prepended; if there is no port suffix, C<:8010> is appended.)

=cut

1;
