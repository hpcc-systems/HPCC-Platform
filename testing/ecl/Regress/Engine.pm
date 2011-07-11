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

package Regress::Engine;

=pod

=head1 NAME

Regress::Engine - perl module used by runregress to drive the process

=head1 SYNOPSIS

my $engine = Regress::Engine->new($options)

$engine->listconfigs();

$engine->init();

$engine->prepare();

$engine->run()

$engine->report()

my %opts = $engine->options();

$engine->normal_copy($infile, $outfile, $postfilter);

$engine->note_to_run($run);

$engine->note_done_run($run, $wuid);

$engine->record_error($path, $message);

$engine->execute($params);

$engine->launch($command, $args);

$engine->error($msg);

$engine->warning($msg);

$engine->log_write($msg, [$fileonly]);

$engine->executable_name($name);

=cut

use strict;
use warnings;
use Cwd qw(getcwd);
use Config::Simple qw();
use File::Spec::Functions qw(curdir catfile splitpath);
use File::Path qw(rmtree);
use POSIX qw(localtime strftime);
use Regress::Prepare qw();
use Regress::EclPlus qw();
use Regress::RoxieConfig qw();
use Regress::Execute qw();
use Regress::ReportList qw();
use Exporter;
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item my $engine = Regress::Engine->new($options)

Takes a hash of options: these are all the options (in their longest forms) taken by L<runregress> except B<help> and B<listreports>, and also B<configuration> for the argument of L<runregress>. Reads the configuration file. Returns an object which can drive the execution.

=cut

sub new($$)
{
    my ($class, $self) = @_;
    bless($self, $class);
    $self->_read_config();
    return $self;
}

=pod

=item $engine->init()

Readies the engine: modifies and checks some values, and writes a list of values to standard output.

=cut

sub init($)
{
    my ($self) = @_;
    $self->_start_logging();
    $self->_set_defaults();
    $self->_regularize_values();
    $self->_check_setup_generate();
    $self->_check_cluster_values();
    $self->_check_suite();
    $self->_calculate_values();
    $self->_gather_cluster_info();
    $self->_record_settings();
    $self->{execute} = Regress::Execute->new($self, $self->{parallel_queries});
    $self->{reportlist} = Regress::ReportList->new();
}

=pod

=item $engine->listconfigs()

Logs a description of the available configurations.

=cut

sub listconfigs($)
{
    my ($self) = @_;
    my @lines = map($self->_config_desc($_), sort(keys(%{$self->{configinfo}})));
    my $txt = join("\n", @lines);
    $self->log_write("Configurations:\n$txt\n");
}

=pod

=item $engine->prepare();

Prepares the regression suite (unless the B<norun> option is true, in which case it just notes any differing settings).

=cut

sub prepare($)
{
    my ($self) = @_;
    my $prepare = Regress::Prepare->new($self);
    if($self->{norun})
    {
        $self->_load_saved_settings($prepare);
        $self->{report_queries} = $prepare->list_queries();
    }
    else
    {
        $self->_purge();
        $self->log_write("Generating queries to suite directory $self->{suite}");
        $prepare->prepare();
    }
}

=pod

=item $engine->run()

Runs a prepared regression suite (unless the B<norun> option is true).

=cut

sub run($)
{
    my ($self) = @_;
    return if($self->{norun} || $self->{preview});
    $self->log_write("Submitting queries");
    $self->error("No queries generated to run") unless($self->{to_run});
    my $num = @{$self->{to_run}};
    $self->error("No queries generated to run") unless($num);

    my $wuidfile = catfile($self->{suite}, 'wuids.csv');
    open($self->{wuidout}, '>', $wuidfile) or $self->error("Could not write $wuidfile: $!");

    my $submit = ($self->{type} eq 'roxie') ? Regress::RoxieConfig->new($self) : Regress::EclPlus->new($self);
    my $seq = 0;
    foreach my $run (@{$self->{to_run}})
    {
        ++$seq;
        my (undef, undef, $filename) = splitpath($run->{path});
        $self->log_write("Submit $filename #$seq/$num");
        $submit->submit($run, $seq);
    }
    $self->{execute}->wait_async();

    close($self->{wuidout});
}

=pod

=item $engine->report()

Generates reports on the regression suite results.

=cut

sub report($)
{
    my ($self) = @_;
    return if($self->{preview});
    $self->log_write("Generating reports");
    my $types = _split_list($self->{report});
    my @reports;
    foreach my $type (@$types)
    {
        my $report = $self->{reportlist}->load($type, $self);
        $self->error($report) unless(ref($report));
        my $problem = $report->precheck();
        $self->error("Problem with report $type: $problem") if($problem);
        push(@reports, $report);
    }
    foreach my $report (@reports)
    {
        $self->log_write("Generating report: $report->{typename}");
        $report->generate();
    }
    $self->log_write("Done");
}

=pod

=item my %opts = $engine->options();

Returns a hash containing the options passed to L<new()> and the variables read from the configuration file.

=cut

sub options($)
{
    my ($self) = @_;
    return %$self;
}

=pod

=item $engine->normal_copy($infile, $outfile, $postfilter);

Takes two file paths. Copies to the latter, normalizing converting CRLF into LF as it goes. Writes a missing file error to the file if the source does not exist. If the postfilter argument is defined, it references a list of objects whose apply methods will be called on each line of the file.

=cut

sub normal_copy($$$$)
{
    my ($self, $infile, $outfile, $postfilter) = @_;
    open(OUT, '>', $outfile) or $self->error("Could not read $outfile: $!");
    if(-e $infile)
    {
        open(IN, '<', $infile) or $self->error("Could not read $infile: $!");
        while(<IN>)
        {
            s/\r\n/\n/g;
            _apply_filters($postfilter, \$_) if($postfilter);
            print(OUT);
        }
        close(IN);
    }
    else
    {
        print(OUT "<ERROR><RUNREGRESS MISSING_FILE=\"$infile\"/></ERROR>\n");
    }
    close(OUT);
}

=pod

=item $engine->note_to_run($run);

Notes a query to be run. Takes a hash containing C<query>, the query name, C<variant>, the variant name (if applicable), C<path>, the ECL path, C<cluster>, the cluster name, and C<outpath>, the output path.

=cut

sub note_to_run($$)
{
    my ($self, $run) = @_;
    push(@{$self->{to_run}}, $run);
}

=pod

=item $engine->note_done_run($run, $wuid);

Notes a query has been run. Takes the sequence number, and the WUID if known.

=cut

sub note_done_run($$$)
{
    my ($self, $seq, $wuid) = @_;
    my $run = $self->{to_run}->[$seq-1];
    print({$self->{wuidout}} join(',', $run->{query}, $run->{variant} || '', $wuid || ''), "\n");
}

=pod

=item $engine->record_error($path, $message);

Takes a path and an error message. Appends a message reporting the error message, in XML format, to the file.

=cut

sub record_error($$$)
{
    my ($self, $path, $message) = @_;
    open(ERROUT, '>>', $path) or $self->{engine}->error("Could not write $path: $!");
    print(ERROUT "<ERROR><RUNREGRESS>$message</RUNREGRESS></ERROR>\n");
    close(ERROUT);
}

=pod

=item $engine->error($msg);

Takes a string describing an error. Logs the error and terminates.

=cut

sub error($$)
{
    my ($self, $msg) = @_;
    $self->log_write("ERROR: $msg");
    $self->{execute}->killall() if($self->{execute});
    exit(2);
}

=pod

=item $engine->warning($msg);

Takes a string describing a warning. Logs it, to STDERR and the log file.

=cut

sub warning($$)
{
    my ($self, $msg) = @_;
    chomp($msg);
    $self->log_write("WARNING: $msg");
}

=pod

=item $engine->log_write($msg, [$fileonly]);

Takes a string. Logs it, to STDERR and the log file. If the optional second argument is true, logs it to the file only.

=cut

sub log_write($$;$)
{
    my ($self, $txt, $fileonly) = @_;
    print(STDERR "$txt\n") unless($fileonly);
    return unless($self->{logfile});
    if(open(LOGOUT, '>>', $self->{logfile}))
    {
        print(LOGOUT "$txt\n");
        close(LOGOUT);
    }
    else
    {
        print(STDERR "ERROR: could not append to $self->{logfile}\n");
        $self->{execute}->killall() if($self->{execute});
        exit(2);
    }
}

=pod

=item $engine->executable_name($name);

Takes the name of an executable expected to be in the regression suite directory and returns a path for it, appending '.exe' if we think we're on windows.

=cut

sub executable_name($$)
{
    my ($self, $base) = @_;
    $base .= '.exe' if($self->{iamwindows});
    my $eclplus = catfile($self->{testdir}, $base);
}

=pod

=item $engine->execute($params, ...)

Intended for use within L<report>. Takes a list of hashes in which C<command> gives the command to run, C<args> refers to a list of arguments, and C<seq> gives a sequence number. Normally, executes it and waits for completion. If the argument hash contains an C<output> then standard output is redirected: if its value is a scalar then it names a file to receive the output, if its value is a subroutine reference then that subroutine is called with the output. If the engine's B<parallel_queries> option is set to a value greater than 1, it will launch that many in the background before it starts blocking, and will allow that many to run at any one time, and at the end of L<report> it will wait for them all to terminate. If the argument hash has a C<done_callback> key its value should be a subroutine reference, which will be called when the command completes (this is important for post-processing in the asynchronous case). This subroutine is called with three arguments: the sequence number; a message describing the termination state (undef if it terminated normally with a zero exit code); and a flag showing whether it terminated abnormally (this is I<not> set by a normal termination with nonzero exit status). The commands are run in seqeuence, stopping if any terminate abnormally or with a nonzero exit status.

=cut

sub execute($@)
{
    my ($self, @paramlist) = @_;
    for my $i (1..$#paramlist)
    {
        $paramlist[$i-1]->{nxt} = $paramlist[$i];
    }
    $self->{execute}->run($paramlist[0]);
}

=pod

=item $engine->launch($command, $args);

Takes a command and a reference to a list of arguments. Executes it in the background and returns.

=cut

sub launch($$$)
{
    my ($self, $command, $args) = @_;
    $self->{execute}->launch($command, $args);
}

# PRIVATE

sub _read_config($)
{
    my ($self) = @_;
    my $configfiles = _split_list($self->{configfile});
    $self->error("No configuration files specified") unless(@$configfiles);
    my $seenblock = 0;
    foreach my $configfile (@$configfiles)
    {
        my $opt = ($configfile =~ s/\?$//);
        next if($opt && !(-e $configfile));
        my $cfgreader = new Config::Simple();
        my $cfgdata = $cfgreader->read($configfile) or $self->error("Could not read config file $configfile: " . $cfgreader->error());
        my @cfgnames = keys(%$cfgdata);
        $self->_get_configuration_info($configfile, $cfgreader, \@cfgnames);
        my $cfgsyntax = lc($cfgreader->syntax());
        $self->error("Config file $configfile had unexpected syntax ($cfgsyntax)") unless($cfgsyntax eq 'ini');
        if($self->{configuration})
        {
            my $seen = $self->_read_config_block($configfile, $cfgreader, $self->{configuration});
            $seenblock |= $seen;
        }
        $self->_read_config_block($configfile, $cfgreader, '*');
    }
    $self->error("No configuration block $self->{configuration} seen (in $self->{configfile})") if($self->{configuration} && !$seenblock);
}

sub _get_configuration_info($$$$)
{
    my ($self, $filename, $cfgreader, $cfgnames) = @_;
    foreach my $cfgname (@$cfgnames)
    {
        next if($cfgname eq '*');
        my $cfg = $cfgreader->get_block($cfgname);
        push(@{$self->{configinfo}->{$cfgname}->{configfiles}}, $filename);
        foreach my $key qw(type cluster roxieserver setup_generate)
        {
            next if(defined($self->{configinfo}->{$cfgname}->{$key}));
            $self->{configinfo}->{$cfgname}->{$key} = $cfg->{$key} if($cfg->{$key});
        }
    }
}

sub _read_config_block($$$$)
{
    my ($self, $filename, $cfgreader, $block) = @_;
    my $cfg = $cfgreader->get_block($block);
    return 0 unless(%$cfg);
    $self->error("Unexpected value for type in global block of config file $filename") if(($block eq '*') && $cfg->{type});
    $self->error("Unexpected value for cluster in global block of config file $filename") if(($block eq '*') && $cfg->{cluster});
    foreach my $key (keys(%$cfg))
    {
        next if(defined($self->{$key}));
        $self->{$key} = $cfg->{$key};
    }
    return 1;
}

sub _config_desc($$)
{
    my ($self, $name) = @_;
    my $cfg = $self->{configinfo}->{$name};
    return ("$name: setup only") if($cfg->{setup_generate}); 
    return ("$name: unknown type") unless($cfg->{type});
    return ("$name: type=$cfg->{type} roxieserver=" . ($self->{roxieserver} || 'unknown')) if($cfg->{type} eq 'roxie');
    return ("$name: type=$cfg->{type} cluster=" . ($self->{cluster} || 'unknown'));
}

sub _start_logging($)
{
    my ($self) = @_;

    unless($self->{logfile})
    {
        my $logbase = $self->{configuration} || ($self->{setup_generate} && 'setup_generate') || $self->error('Configuration not specified but setup_generate not set');
        $self->{logfile} = "$logbase.log";
    }

    if(-e $self->{logfile})
    {
        unlink($self->{logfile}) or $self->warning("Could not remove $self->{logfile}");
    }

    my $curtime = _current_time();
    $self->log_write("Starting runregress at $curtime");
    $SIG{__WARN__} = sub { $self->warning(@_); };
    $self->log_write("Writing log to $self->{logfile}");
}

sub _set_defaults($)
{
    my ($self) = @_;
    $self->{deploy_roxie_queries} = 'yes' unless(defined($self->{deploy_roxie_queries}));
    $self->{report} = 'Default' unless(defined($self->{report}));
    $self->{time} = _current_time();
    $self->{iamwindows} = $self->_am_i_windows();
    $self->{setup_file_location} = '' unless(defined($self->{setup_file_location}));
}

sub _regularize_values($)
{
    my ($self) = @_;
    $self->{type} = lc($self->{type});
    $self->{os} = lc($self->{os});
    $self->{class} = lc($self->{class});
    $self->{setup_generate} = 'on' if($self->{setup_generate});
    $self->{norun} = 'on' if($self->{norun});
    $self->{deploy_roxie_queries} = 'no' unless(($self->{deploy_roxie_queries} eq 'yes') || ($self->{deploy_roxie_queries} eq 'run'));
}

sub _check_setup_generate($)
{
    my ($self) = @_;
    if($self->{setup_generate})
    {
        $self->error("Cannot run setup on roxie type") if($self->{type} eq 'roxie');
        $self->error("Cannot combine option -class with option -setup or config value setup_generate") if($self->{class});
        $self->error("Cannot combine option -query with option -setup or config value setup_generate") if(defined($self->{query}));
        $self->error("Cannot combine option -variant with option -setup or config value setup_generate") if(defined($self->{variant}));
        $self->{suite} = 'setup_generate';
        $self->{report} = 'Summary';
    }
}

sub _check_cluster_values($)
{
    my ($self) = @_;
    $self->error("Cluster name $self->{cluster} contains illegal characters") if($self->{cluster} && ($self->{cluster} =~ /[^[:alnum:,-]_]/));
    $self->{setup_clusters} = $self->{cluster} unless($self->{setup_clusters});
    if($self->{type} eq 'roxie')
    {
        $self->error("Config does not provide value for setup_clusters for roxie tests") unless($self->{setup_clusters});
        $self->error("Config does not supply required roxieconfig value") unless($self->{roxieconfig} || ($self->{deploy_roxie_queries} eq 'no'));
        $self->error("Config does not supply required roxieserver value") unless($self->{roxieserver} || $self->{deploy_roxie_queries} eq 'run');
    }
    else
    {
        if($self->{setup_generate})
        {
            $self->error("Config does not provide value for setup_clusters and is setup_generate") unless($self->{setup_clusters});
        }
        else
        {
            $self->error("Config does not provide value for type and is not setup_generate") unless($self->{type});
            $self->error("Config does not provide value for cluster and is not setup_generate") unless($self->{cluster});
        }
    }
    $self->error("Config does not provide value for owner") unless($self->{owner});
    $self->_promptpw() unless($self->{password} || $self->{preview} || $self->{norun} || (($self->{type} eq 'roxie') && ($self->{deploy_roxie_queries} eq 'no')));
}

sub _check_suite($)
{
    my ($self) = @_;
    if(!$self->{suite})
    {
        $self->error("Suite name missing") unless($self->{configuration});
        $self->{suite} = $self->{configuration};
    }
    $self->error("Suite name $self->{suite} is reserved") if(grep(($self->{suite} eq $_), qw(setup hthor thor roxie windows linux)));
    $self->error("Suite name $self->{suite} contains illegal characters") if($self->{suite} =~ /[^[:alnum:,-]_]/);
}

sub _calculate_values($)
{
    my ($self) = @_;
    $self->error("Empty query list specified") if(defined($self->{query}) && !$self->{query});
    $self->{query} = _split_list($self->{query}) if($self->{query});
    $self->error("Empty variant list specified") if(defined($self->{variant}) && !$self->{variant});
    $self->{variant} = _split_list($self->{variant}) if($self->{variant});
    $self->{setup_clusters} = _split_list($self->{setup_clusters});
    my @bad_clusters = grep(/[^[:alnum:,-]_]/, @{$self->{setup_clusters}});
    $self->error("Setup cluster name(s) " . join(',', @bad_clusters) . " contain illegal characters") if(@bad_clusters);
    $self->{curdir} = getcwd();
    $self->{testdir} = curdir() unless($self->{testdir});
    eval { require Sys::Hostname; $self->{hostname} = Sys::Hostname::hostname(); };
}

sub _split_list($)
{
    my ($in) = @_;
    return [] unless($in);
    return $in if(ref($in));
    my @list = split(/[,\s]+/, $in);
    return \@list;
}

sub _gather_cluster_info($)
{
    my ($self) = @_;
    my %clusterinfo;
    foreach my $cfgname (keys(%{$self->{configinfo}}))
    {
        my $cfg = $self->{configinfo}->{$cfgname};
        my $cluster = $cfg->{cluster};
        my $type = $cfg->{type};
        next unless($cluster && $type);
        push(@{$clusterinfo{$cluster}->{configs}}, $cfgname);
        my $oldtype = $clusterinfo{$cluster}->{type};
        if($oldtype)
        {
            unless($oldtype eq $type)
            {
                my $configlist = join(', ', @{$clusterinfo{$cluster}->{configs}});
                $self->error("Bad configuration: the cluster $cluster is assigned different types in different configurations ($configlist)");
            }
        }
        else
        {
            $clusterinfo{$cluster}->{type} = $type;
        }
    }

    foreach my $cluster (@{$self->{setup_clusters}})
    {
        my $type = $clusterinfo{$cluster}->{type};
        unless($type)
        {
            $self->warning("Could not determine type of setup cluster $cluster from configuration (it appears not to be used as the active cluster in any configuration), assuming thor");
            $type = 'thor';
        }
        $self->{setup_cluster_types}->{$cluster} = $type;
    }
}

sub _load_saved_settings($$)
{
    my ($self, $prepare) = @_;
    my $saved = $prepare->load_settings();
    foreach my $key (keys(%$saved))
    {
        my $val = $saved->{$key};
        $val = join(',', @$val) if(ref($val) eq 'ARRAY');
        $self->{rundesc}->{$key} = $val unless(defined($self->{desc}->{$key}) && ($self->{desc}->{$key} eq $val));
    }
    foreach my $key (keys(%{$self->{desc}}))
    {
        $self->{rundesc}->{$key} = '[not set]' unless(defined($saved->{$key}));
    }
    if($self->{rundesc})
    {
        my @lines = map("    $_=$self->{rundesc}->{$_}", sort(keys(%{$self->{rundesc}})));
        my $txt = join("\n", @lines);
        $self->log_write("Settings and values when suite was run, where different:\n$txt\n");
    }
}

sub _record_settings($)
{
    my ($self) = @_;
    foreach my $key (keys(%$self))
    {
        next if(lc($key) eq 'configfile');
        my $val = (lc($key) eq 'password' ? '********' : $self->{$key});
        next unless($val);
        $val = join(',', @$val) if(ref($val) eq 'ARRAY');
        $self->{desc}->{$key} = $val if($val && !ref($val));
    }

    my $svnurl = '$HeadURL: https://svn.br.seisint.com/ecl/trunk/testing/ecl/Regress/Engine.pm $';
    my $svnrev = '$LastChangedRevision: 65373 $';
    if($svnurl =~ /^\$HeadURL:.*\/build_(.+?)\/.*\$/)
    {
        $self->{desc}->{svnbuild} = "$1";
    }
    elsif($svnrev =~ /^\$LastChangedRevision: *(.+?) *\$/)
    {
        $self->{desc}->{svnrev} = "$1";
    }

    my @lines = map("    $_=$self->{desc}->{$_}", sort(keys(%{$self->{desc}})));
    my $txt = join("\n", @lines);
    $self->log_write("Settings and values:\n$txt\n");
}

sub _promptpw($)
{
    my ($self) = @_;
    eval { require Term::Prompt; };
    $self->error("Config does not provide value for password and cannot prompt on terminal (Term::Prompt module unavailable)") if($@);
    my $pw = Term::Prompt::prompt('P', "Password for $self->{owner}:", 'Please enter password', '');
    $self->{password} = $pw or $self->error("Blank password given at prompt");
}

sub _current_time()
{
    return strftime('%Y-%m-%d %H:%M:%S%z', localtime(time()));
}

sub _am_i_windows($)
{
    my ($self) = (@_);
    if($^O =~ /linux/)
    {
        return 0;
    }
    elsif($^O =~ /(cygwin|mswin)/i)
    {
        return 1;
    }
    else
    {
        $self->warning("Could not establish OS from reported value '$^O', assuming linux: PLEASE REPORT THIS WARNING");
        return 0;
    }
}

sub _purge($)
{
    my ($self) = @_;
    return unless(-e $self->{suite});
    if($self->{purge} && lc($self->{purge} eq 'move'))
    {
        my $backup = "$self->{suite}.bak";
        $self->log_write("Backing up old suite directory $self->{suite} to $backup");
        rmtree($backup);
        $self->error("Failed to remove backup suite directory $backup") if(-e $backup);
        rename($self->{suite}, $backup) or $self->error("Could not rename $self->{suite} to $backup: $!");
    }
    else
    {
        $self->log_write("Removing old suite directory $self->{suite}");
        rmtree($self->{suite});
    }
    $self->error("Failed to move or remove suite directory $self->{suite}") if(-e $self->{suite});
}

sub _apply_filters($$)
{
    my ($filters, $ref) = @_;
    $_->apply($ref) foreach(@$filters);
}

=pod

=back

=cut

1;
