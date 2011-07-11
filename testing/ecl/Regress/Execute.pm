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

package Regress::Execute;

=pod

=head1 NAME
    
Regress::Execute - perl module used by runregress to execute commands, synchronously or asynchronously

=head1 SYNOPSIS

my $execute = Regress::Execute->new($max);

my $ret = $execute->run($params);

$execute->wait_async();

$execute->killall();

Regress::Execute::launch($command, $args);

=cut
    
use strict;
use warnings;
use IPC::Run qw();
use POSIX qw(:sys_wait_h);
use Exporter;
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item my $execute = Regress::Execute->new($max);

Takes a L<Regress::Engine> object and the maximum number of processes to spawn at once. Returns an object to execute them.

=cut

sub new($$)
{
    my ($class, $engine, $max) = @_;
    my $self = {engine => $engine,
                max => $max || 1,
                count => 0,
                running => [],
                isposix => _isposix()};
    return bless($self, $class);
}

=pod

=item my $ret = $execute->run($params);

Takes a hash as passed to L<Regress::Engine::execute>, except that C<nxt> gives another similar hash to run on completion. If the maximum number of queries is greater than 1, this is done asynchronously.

=cut

sub run($$)
{
    my ($self, $params) = @_;
    $self->_add_harness($params);
    if($self->{max} > 1)
    {
        $self->_wait($self->{max}-1);
        push(@{$self->{running}}, $params);
        ++$self->{count};
        $self->{engine}->log_write("Command: $params->{commandlog}") if($self->{engine}->{verbose});
        eval { IPC::Run::start($params->{harness}); };
        if($@)
        {
            $params->{termerror} = "failed to start: $@" ;
            $params->{termbad} = 1;
        }
    }
    else
    {
        while($params)
        {
            $self->{engine}->log_write("Command: $params->{commandlog}") if($self->{engine}->{verbose});
            eval { IPC::Run::run($params->{harness}); };
            if($@)
            {
                $params->{termerror} = "failed to start: $@" ;
                $params->{termbad} = 1;
            }
            $self->_handle_return($params);
            &{$params->{done_callback}}($params->{seq}, $params->{termerror}, $params->{termbad}) if($params->{done_callback});
            last if($params->{termerror});
            $params = $params->{nxt};
        }
    }
}

=pod

=item $execute->wait_async();

Waits for all running asynchronous processes to complete.

=cut

sub wait_async($)
{
    my ($self) = @_;
    $self->_wait(0);
}

=pod

=item $execute->killall();

Kills any processes still running.

=cut

sub killall($)
{
    my ($self) = @_;
    IPC::Run::kill_kill($_->{harness}) foreach (@{$self->{running}});
}

=pod

=item Regress::Execute::launch($command, $args);

Takes a command and a reference to a list of arguments. Executes it in the background and returns.

=cut

sub launch($$$)
{
    my ($self, $command, $args) = @_;
    eval { IPC::Run::start([$command, @$args]); };
    $self->{engine}->warning("$command launch: failed to start: $@\n") if($@);
}

#PRIVATE

sub _add_harness($$)
{
    my ($self, $params) = @_;
    while($params)
    {
        my $cmd = [$params->{command}, @{$params->{args}}];
        my @runargs = ($cmd);

        $params->{commandlog} = join(' ', @$cmd);
        if($params->{output})
        {
            push(@runargs, '>', $params->{output});
            $params->{commandlog} = ($params->{commandlog} . ' > ' . (ref($params->{output}) ? '[internal]' : $params->{output}));
        }
        $params->{harness} = IPC::Run::harness(@runargs);
        $params = $params->{nxt};
    }
}

sub _wait($$)
{
    my ($self, $limit) = @_;
    while($self->{count} > $limit)
    {
        my $i = 0;
        while($i < $self->{count})
        {
            my $try = $self->{running}->[$i];
            my $more = IPC::Run::pumpable($try->{harness});
            eval { $more = $more && IPC::Run::pump_nb($try->{harness}); };
            $more = 0 if($@); #pump dies if the process was finished since call to pumpable (possible since it yields)
            if($more)
            {
                ++$i;
            }
            else
            {
                IPC::Run::finish($try->{harness});
                $self->_handle_return($try);
                &{$try->{done_callback}}($try->{seq}, $try->{termerror}, $try->{termbad}) if($try->{done_callback});
                if($try->{nxt} && !$try->{termerror})
                {
                    $self->{running}->[$i] = $try->{nxt};
                    $self->{engine}->log_write("Command: $try->{nxt}->{commandlog}") if($self->{engine}->{verbose});
                    eval { IPC::Run::start($try->{nxt}->{harness}); };
                    $self->{engine}->warning("$try->{nxt}->{command} #$try->{nxt}->{seq}: failed to start: $@\n") if($@);
                    ++$i;
                }
                else
                {
                    splice(@{$self->{running}}, $i, 1);
                    return if(--$self->{count} <= $limit);
                }
            }
        }
        sleep(1);
    }
}

sub _handle_return($$)
{
    my ($self, $params) = @_;
    my $ret = $params->{harness}->full_result();
    if($ret == -1)
    {
        #for safety, but IPC::Run should not have allowed us to get here
        $params->{termerror} = "failed to execute";
        $params->{termbad} = 1;
    }
    elsif(!$self->{isposix})
    {
        #assume that full_result value is meaningful by itself, as on ActivePerl
        $params->{termerror} = "returned $ret" if($ret);
    }
    elsif(WIFEXITED($ret))
    {
        my $status = WEXITSTATUS($ret);
        $params->{termerror} = "exited with status $status" if($status);
    }
    elsif(WIFSIGNALED($ret))
    {
        my $signal = WTERMSIG($ret);
        $params->{termerror} = "terminated by signal $signal";
        $params->{termbad} = 1;
    }
    else
    {
        $params->{termerror} = "terminated with unknown status, wait function returned $ret";
        $params->{termbad} = 1;
    }
    $self->{engine}->warning("$params->{command} #$params->{seq}: $params->{termerror}") if($params->{termerror});
}

sub _isposix()
{
    eval { WIFEXITED(0); WEXITSTATUS(0); WIFSIGNALED(0); WTERMSIG(0); };
    return(!$@);
}

=pod

=back

=cut

1;
