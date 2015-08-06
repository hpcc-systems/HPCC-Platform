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

package Regress::Report::Default;

=pod

=head1 NAME
    
Regress::Report::Default - perl module used by runregress, subclass of L<Regress::Report>

=cut

use strict;
use warnings;
use Regress::Report::Summary qw();
use Exporter;
our @ISA = qw(Regress::Report::Summary);

#PUBLIC

sub describe($)
{
    my ($type) = @_;
    return undef;
}

#PROTECTED

sub _end($)
{
    my ($self) = @_;
    print("To generate a full report:\n    use the report option or configuration variable.\n");
    print("To generate a report on these results without rerunning the suite:\n    use the norun option.\n");
    print("To see a list of available report types:\n    use the listreports option.\n");
    print("\n");
}

=pod

=head1 DESCRIPTION

This report type is intended for use as the default. It writes a summary of the results to standard output, followed by a message telling the user how to generate other reports. The summary is identical to that of L<Regress::Report::Summary>.

=cut

1;
