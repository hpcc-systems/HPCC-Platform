/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

package JoinTest;

use strict;
use warnings;
use Exporter;
our @ISA = qw(Exporter);
our @EXPORT;

our @activities = qw(JOIN DENORMALIZE DENORMALIZEGROUP);
our @types = (0, qw(LOOKUP LOOKUP_MANY ALL));
our @inouts = (0, map({my $a = $_; map({[$a, $_]} qw(OUTER ONLY))} qw(LEFT RIGHT FULL)));
our @limits = (0, qw(KEEP ATMOST LIMITSKIP LIMITONFAIL LIMITFAIL));
our @xfmskips = (0, 1);
our @parallels = (0, 1);

our @factors = ({name => 'activity', vals => \@JoinTest::activities},
                {name => 'type', vals => \@JoinTest::types},
                {name => 'inout', vals => \@JoinTest::inouts},
                {name => 'limit', vals => \@JoinTest::limits},
                {name => 'xfmskip', vals => \@JoinTest::xfmskips},
                {name => 'parallel', vals => \@JoinTest::parallels});

my %_activitycodes = (JOIN => 'join', DENORMALIZE => 'deno', DENORMALIZEGROUP => 'dngr');
my %_activitycommands = (JOIN => 'JOIN', DENORMALIZE => 'DENORMALIZE', DENORMALIZEGROUP => 'DENORMALIZE');
my %_typecodes = (0 => '__', LOOKUP => 'l1', LOOKUP_MANY => 'lm', ALL => 'al');
my %_limitcodes = (0 => '__', KEEP => 'kp', ATMOST => 'at', LIMITSKIP => 'ls', LIMITONFAIL => 'lo', LIMITFAIL => 'lf');
my @_bads = ({type => 'LOOKUP', limit => 'KEEP'},
             {type => 'LOOKUP', limit => 'ATMOST'},
             {type => 'LOOKUP', limit => 'LIMITSKIP'},
             {type => 'LOOKUP', limit => 'LIMITONFAIL'},
             {type => 'LOOKUP', limit => 'LIMITFAIL'},
             {type => 'LOOKUP', inout => 'RIGHT_OUTER'},
             {type => 'LOOKUP', inout => 'RIGHT_ONLY'},
             {type => 'LOOKUP', inout => 'FULL_OUTER'},
             {type => 'LOOKUP', inout => 'FULL_ONLY'},
             {type => 'LOOKUP_MANY', inout => 'RIGHT_OUTER'},
             {type => 'LOOKUP_MANY', inout => 'RIGHT_ONLY'},
             {type => 'LOOKUP_MANY', inout => 'FULL_OUTER'},
             {type => 'LOOKUP_MANY', inout => 'FULL_ONLY'},
             {type => 'ALL', limit => 'ATMOST'},
             {type => 'ALL', limit => 'LIMITSKIP'},
             {type => 'ALL', limit => 'LIMITONFAIL'},
             {type => 'ALL', limit => 'LIMITFAIL'},
             {type => 'ALL', inout => 'RIGHT_OUTER'},
             {type => 'ALL', inout => 'RIGHT_ONLY'},
             {type => 'ALL', inout => 'FULL_OUTER'},
             {type => 'ALL', inout => 'FULL_ONLY'},
             {inout => 'RIGHT_OUTER', limit => 'KEEP'},
             {inout => 'RIGHT_OUTER', limit => 'ATMOST'},   
             {inout => 'FULL_OUTER', limit => 'KEEP'},
             {inout => 'FULL_OUTER', limit => 'ATMOST'},
             {inout => 'LEFT_ONLY', limit => 'KEEP'},
             {inout => 'LEFT_ONLY', limit => 'LIMITSKIP'},
             {inout => 'LEFT_ONLY', limit => 'LIMITONFAIL'},
             {inout => 'LEFT_ONLY', limit => 'LIMITFAIL'},
             {inout => 'RIGHT_ONLY', limit => 'KEEP'},
             {inout => 'RIGHT_ONLY', limit => 'ATMOST'},
             {inout => 'RIGHT_ONLY', limit => 'LIMITSKIP'},
             {inout => 'RIGHT_ONLY', limit => 'LIMITONFAIL'},
             {inout => 'RIGHT_ONLY', limit => 'LIMITFAIL'},
             {inout => 'FULL_ONLY', limit => 'KEEP'},
             {inout => 'FULL_ONLY', limit => 'ATMOST'},
             {inout => 'FULL_ONLY', limit => 'LIMITSKIP'},
             {inout => 'FULL_ONLY', limit => 'LIMITONFAIL'},
             {inout => 'FULL_ONLY', limit => 'LIMITFAIL'},
             );

my @_nothors = ({activity => 'DENORMALIZE', type => 'LOOKUP'},
                {activity => 'DENORMALIZE', type => 'LOOKUP_MANY'},
                {activity => 'DENORMALIZE', type => 'ALL'},
                {activity => 'DENORMALIZE', limit => 'ATMOST'},
                {activity => 'DENORMALIZE', limit => 'LIMITSKIP'},
                {activity => 'DENORMALIZE', limit => 'LIMITONFAIL'},
                {activity => 'DENORMALIZE', limit => 'LIMITFAIL'},
                {activity => 'DENORMALIZE', inout => 0},
                {activity => 'DENORMALIZE', inout => 'LEFT_ONLY'},
                {activity => 'DENORMALIZE', inout => 'RIGHT_OUTER'},
                {activity => 'DENORMALIZE', inout => 'RIGHT_ONLY'},
                {activity => 'DENORMALIZE', inout => 'FULL_OUTER'},
                {activity => 'DENORMALIZE', inout => 'FULL_ONLY'},
                {activity => 'DENORMALIZEGROUP'});

sub new($% )
{
    my ($class, %opts) = @_;
    my $self = \%opts;
    bless($self, $class);
    $self->_setdesc();
    $self->_setcode();
    $self->_setmatchxfm();
    return $self;
}

sub _setdesc($ )
{
    my ($self) = @_;
    my @features = ($self->{activity});
    push(@features, $self->{type}) if($self->{type});
    push(@features, join('_', @{$self->{inout}})) if($self->{inout});
    push(@features, $self->{limit}) if($self->{limit});
    push(@features, 'XFMSKIP') if($self->{xfmskip});
    push(@features, 'PAR') if($self->{parallel});
    $self->{desc} = join('_', @features);
}

sub _actcode($ )
{
    my ($self) = @_;
    my $ac = $_activitycodes{$self->{activity}} or die("unknown activity $self->{activity}");
    return $ac;
}

sub _typecode($ )
{
    my ($self) = @_;
    my $tc = $_typecodes{$self->{type}} or die("unknown type $self->{type}");
    return $tc;
}

sub _inoutcode($ )
{
    my ($self) = @_;
    return $self->{inout} ? (substr($self->{inout}->[0], 0, 1) . substr($self->{inout}->[1], 0, 2)) : 'INN';
}

sub _limitcode($ )
{
    my ($self) = @_;
    my $lc = $_limitcodes{$self->{limit}} or die("unknown limit type $self->{limit}");
    return $lc;
}

sub _xfmcode($ )
{
    my ($self) = @_;
    return ($self->{xfmskip} ? 'xs' : '__');
}

sub _parcode($ )
{
    my ($self) = @_;
    return ($self->{parallel} ? 'p' : '_');
}

sub _setcode($ )
{
    my ($self) = @_;
    my @features = ($self->_actcode(), $self->_typecode(), $self->_inoutcode(), $self->_limitcode(), $self->_xfmcode(), $self->_parcode());
    $self->{code} = join('_', @features);
}

sub _setmatchxfm($ )
{
    my ($self) = @_;
    $self->{matcharg} = ($self->{type} eq 'ALL') ? 'allmatch' : 'match';
    $self->{xfmarg} = 'xfm';
    my $xfmrightarg := 'RIGHT';
    if($self->{activity} eq 'DENORMALIZEGROUP')
    {
        $self->{xfmarg} .= 'grp';
        $xfmrightarg = 'ROWS(RIGHT)';
    }
    if($self->{xfmskip})
    {
        $self->{matcharg} .= '1';
        $self->{xfmarg} .= 'skip';
    }
    $self->{matcharg} .= '(LEFT, RIGHT)';
    $self->{xfmarg} .= "(LEFT, $xfmrightarg, '$self->{desc}')";
}

sub _matchesConditions($$ )
{
    my ($self, $conds) = @_;
    foreach my $cond (keys(%$conds))
    {
        my $val = $self->{$cond};
        $val = join('_', @$val) if(ref($val));
        return 0 unless($val eq $conds->{$cond});
    }
    return 1;
}

sub forbidden($ )
{
    my ($self) = @_;
    foreach my $bad (@_bads)
    {
        return 1 if($self->_matchesConditions($bad));
    }
    return 0;
}

sub nothor($ )
{
    my ($self) = @_;
    foreach my $nothor (@_nothors)
    {
        return 1 if($self->_matchesConditions($nothor));
    }
    return 0;
}

sub justroxie($ )
{
    my ($self) = @_;
    return $self->{parallel};
}

sub fails($ )
{
    my ($self) = @_;
    return ($self->{limit} eq 'LIMITFAIL');
}

sub _limitargs($ )
{
    my ($self) = @_;
    return("KEEP($self->{keepval})"), if($self->{limit} eq 'KEEP');
    return("ATMOST(match1(LEFT, RIGHT), $self->{limitval})"), if($self->{limit} eq 'ATMOST');
    return("LIMIT($self->{limitval}, SKIP)", "ONFAIL(xfm(LEFT, RIGHT, 'FAILED: $self->{desc}'))"), if($self->{limit} eq 'LIMITSKIP');
    return("LIMIT($self->{limitval})", "ONFAIL(xfm(LEFT, RIGHT, 'FAILED: $self->{desc}'))"), if($self->{limit} eq 'LIMITONFAIL');
    return("LIMIT($self->{limitval})"), if($self->{limit} eq 'LIMITFAIL');
    die("unknown limit type $self->{limit}");
}

sub _args($ )
{
    my ($self) = @_;
    my @args = ('lhs', 'rhs', "$self->{matcharg}");
    push(@args, 'GROUP') if($self->{activity} eq 'DENORMALIZEGROUP');
    push(@args, "$self->{xfmarg}");
    push(@args, split(/_/, $self->{type})) if($self->{type});
    push(@args, ($self->{inout} ? join(' ', @{$self->{inout}}) : 'INNER'));
    push(@args, $self->_limitargs()) if($self->{limit});
    push(@args, 'PARALLEL') if($self->{parallel});
    return join(', ', @args);
}

sub defecl($ )
{
    my ($self) = @_;
    my $actcmd = $_activitycommands{$self->{activity}};
    my $args = $self->_args();
    return("$self->{code} := $actcmd($args)");
}

sub outecl($ )
{
    my ($self) = @_;
    return("OUTPUT($self->{code}, NAMED('$self->{desc}'))");
}

1;
