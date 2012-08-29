################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

package Regress::EclPlus;

=pod

=head1 NAME
    
Regress::EclPlus - perl module used by runregress to execute eclplus

=head1 SYNOPSIS

my $submit = Regress::EclPlus->new($opts);

$submit->submit($run, $seq);

=cut
    
use strict;
use warnings;
use Exporter;
use File::Spec::Functions qw(catfile);
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item my $submit = Regress::EclPlus->new(%options);

Takes a hash of options obtained from a L<Regress::Engine> object. Returns an object used to execute eclplus.

=cut

sub new($$)
{
    my ($class, $engine) = @_;
    my $self = {engine => $engine,
                $engine->options()};
    return bless($self, $class);
}

=pod

=item $submit->submit($run, $seq);

Runs eclplus to submit a query. Takes a hash as passed to L<Regress::Engine::note_to_run>, and a sequence number. Calls L<Regress::Engine::note_done_run> when completed, passing the run hash and the WUID if known.

=cut

sub submit($$$)
{
    my ($self, $run, $seq) = @_;
    my $tmpout = "$run->{outpath}.tmp";
    $self->{seqpaths}->{$seq} = {outpath => $run->{outpath}, tmppath => $tmpout, postfilter => $run->{postfilter}};
    my $command = $self->{eclplus} || $self->{eclpluscmd} || $self->{engine}->executable_name('eclplus');
    my %vals = (format => 'xml',
                -fnoCache => 1,
                -fimportAllModules => 0,
                -fimportImplicitModules => 0,
                noWarnings => 1,
                noInfo => 1,
                cluster => $run->{cluster},
                server => $self->{server},
                owner => $self->{owner},
                password => $self->{password},
                output => $tmpout);
    $vals{ 'query' } = "\@$run->{queryxmlpath}" if (-e $run->{queryxmlpath});
    my $args = ["\@$run->{path}", map("$_=$vals{$_}", sort(keys(%vals)))];
    $self->{engine}->execute({command => $command, 
                              args => $args,
                              output => sub { $self->_scan_for_wuid($seq, @_); },
                              done_callback => sub { $self->_on_complete(@_); },
                              seq => $seq});
}

#PRIVATE

sub _on_complete($$$$)
{
    my ($self, $seq, $termerror, $termbad) = @_;
    my $paths = $self->{seqpaths}->{$seq};
    $self->{engine}->record_error($paths->{tmppath}, $termerror) if($termbad);
    $self->{engine}->warning("No output file created by #$seq") unless(-e $paths->{tmppath});
    $self->{engine}->normal_copy($paths->{tmppath}, $paths->{outpath}, $paths->{postfilter});
    unlink($paths->{tmppath});
    $self->{engine}->note_done_run($seq, $self->{wuids}->{$seq});
    $self->{engine}->log_write("Done #$seq (" . ($termerror ? $termerror : "OK") . ")");
}

sub _scan_for_wuid($$$)
{
    my ($self, $seq, $line) = @_;
    $line =~ s/[\r\n]+$//;
    $self->{engine}->log_write("eclplus output: [$line]", !$self->{engine}->{verbose});
    return if($self->{wuids}->{$seq});
    return unless($line =~ /(W[\d-]+)/);
    $self->{wuids}->{$seq} = $1;
    $self->{engine}->log_write("Created $self->{wuids}->{$seq} for #$seq");
}

=pod

=back

=cut

1;

