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

package Regress::ReportList;

=pod

=head1 NAME

Regress::ReportList - perl module used by runregress to get report types

=head1 SYNOPSIS

my $reports = Regress::ReportList->new();

$reports->print();

my $error = $reports->error($type);

=cut

use strict;
use warnings;
use File::Spec::Functions qw(catfile);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item Regress::ReportList->new();

Returns a new object containing a list of available report types (including broken ones).

=cut

sub new($)
{
    my ($class) = @_;
    my $self = bless({}, $class);
    foreach my $dir (@INC)
    {
        my $pattern = catfile($dir, 'Regress', 'Report', '*.pm');
        $self->_load_type_info($_) foreach (glob($pattern));
    }
    return $self;
}

=pod

=item $reports->print()

Prints a list of report types and descriptions (or errors) to standard output.

=cut

sub print($)
{
    my ($self) = @_;
    eval { require Text::Wrap; };
    my $gotwrap = !$@;

    foreach my $key (sort(keys(%$self)))
    {
        my $type = $self->{$key}->{type};
        my $text = $self->_get_text($self->{$key});
        next unless($text);
        my $fancytext = ($gotwrap ? Text::Wrap::wrap('    ', '    ', $text) : "    $text");
        print("$type\n$fancytext\n\n");
    }
}

=pod

=item my $info = $reports->load($key, $engine);

Takes a type name (case insensitive) and a L<Regress::Engine> object. Returns an object of that report type.

=cut

sub load($$$)
{
    my ($self, $key, $engine) = @_;
    my $info = $self->{lc($key)};
    $engine->error("No such report type $key") unless($info);
    $engine->error("Report type $key unavailable, the module at $info->{path} is broken (see --listreports for details)") if($info->{error});

    my $type = $info->{type};
    eval { require $info->{path}; };
    $engine->error("Report type $key unavailable, the module failed to initialize with the following error message:\n$@") if($@);
    my $report;
    eval { $report = "Regress::Report::$type"->new($type, $engine); };
    $engine->error("Report type $key unavailable, the module's new method failed with the following error message:\n$@") if($@);
    $engine->error("Report type $key unavailable, the module's new method did not return an object") unless($report && ref($report));
    $engine->error("Report type $key unavailable, the module's typename member did not match") unless($report->{typename} eq $type);
    return $report;
}

#PRIVATE

sub _load_type_info($$)
{
    my ($self, $path) = @_;
    my $type = _read_module_name($path);
    if(ref($type))
    {
        my (undef, undef, $ftype) = File::Spec->splitpath($path);
        $ftype =~ s/\.?pm$//;
        $self->_note_bad_type($ftype, $type->{error}, $path);
        return;
    }
    $self->_note_type($type, $path);
}

sub _read_module_name($)
{
    my ($path) = @_;
    open(MOD, '<', $path) or return {error => "The module file could not be opened: $!."};
    my $type;
    while(<MOD>)
    {
        if(/^package Regress::Report::(.*);/)
        {
            $type = $1;
            last;
        }
    }
    close(MOD);
    return ($type || {error => "The module file did not provide a Regress::Report package."});
}

sub _note_type($$$)
{
    my ($self, $type, $path) = @_;
    my $key = lc($type);
    return if($self->{$key} && $self->{$key}->{error});
    $self->{$key} = {type => $type, path => $path};
}

sub _note_bad_type($$$$)
{
    my ($self, $type, $error, $path) = @_;
    my $key = lc($type);
    return if($self->{$key});
    $self->{$key} = {type => $type, error => $error, path => $path};
}

sub _get_text($$)
{
    my ($self, $info) = @_;
    my $type = $info->{type};
    my $path = $info->{path};
    return "DO NOT USE THIS REPORT TYPE.\nThe module at $path is broken. $info->{error}" if($info->{error});
    eval { require $path; };
    if($@)
    {
        $info->{error} = "The module failed to initialize with the following error message.\n$@";
        return "DO NOT USE THIS REPORT TYPE.\nThe module at $path is broken. $info->{error}";
    }
    my $desc;
    eval { $desc = "Regress::Report::$type"->describe(); };
    if($@)
    {
        $info->{error} = "The module's describe method failed with the following error message.\n$@";
        return "DO NOT USE THIS REPORT TYPE.\nThe module at $path is broken. $info->{error}";
    }
    return $desc;
}

1;
