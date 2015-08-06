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
