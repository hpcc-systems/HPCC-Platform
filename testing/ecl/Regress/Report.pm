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

package Regress::Report;

=pod

=head1 NAME
    
Regress::Report - perl module used by runregress as base for classes to generate reports

=head1 SYNOPSIS

my $report = Regress::Report::I<subclass>->new($typename, $engine);

my $problem = $report->precheck();

$report->generate();

my $text = Regress::Report::I<subclass>->describe();

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

=item my $report = Regress::Report::I<subclass>->new($typename, $engine);

Takes the name of the type implemented and a L<Regress::Engine> object. Returns an object used to generate a report. A default constructor is provided by this base class.

=cut

sub new($$)
{
    my ($class, $typename, $engine) = @_;
    my $self = {typename => $typename,
                engine => $engine,
                $engine->options()};
    return bless($self, $class);
}

=pod

=item my $problem = $report->precheck()

Provides an opportunity to carry out quick checks before any reports are generated. When the user has chosen multiple reports, this avoids waiting for earlier reports to generate only to have a later report fail. A false value indicates success. A true value indicates failure, and should be a string describing the problem. This method I<must> be overridden by subclasses, even if only to return a false value.

=cut

sub precheck()
{
    my ($self) = @_;
    return "Coding error: $self->{typename} is not a good report class (did not implement precheck)";
}

=pod

=item $report->generate()

Generates the report. This method I<must> be overridden by subclasses.

=cut

sub generate()
{
    my ($self) = @_;
    $self->{engine}->error("Coding error: $self->{typename} is not a good report class (did not implement generate)");
}

=pod

=item my $text = Regress::Report::I<subclass>->describe();

If the report type is intended for general use, this should return a string describing the report, which will be included in a list of report types. If not, it should return a false value, and the type will be omitted from the list. This method I<must> be overridden by subclasses.

=cut

sub describe($)
{
    my ($type) = @_;
    return "DO NOT USE.\nThis report type is broken. It contains a coding error: the describe function was not implemented.";

}

=pod

=back

=cut

1;
