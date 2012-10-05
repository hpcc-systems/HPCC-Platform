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

package Bundle::Regress;

1;

__END__

=head1 NAME

Bundle::Regress - A bundle to install the modules used by runregress

=head1 SYNOPSIS

perl -MCPAN -e 'install Bundle::Regress'

=head1 CONTENTS

Config::Simple
# Cwd                           - core
# Exporter                      - core
# File::Compare                 - core, required for most report types
# File::Path                    - core
# File::Spec::Functions         - core
# Getopt::Long                  - core
HTML::Entities                  - required for the HTML report type
IPC::Run                        - required
# Pod::Usage                    - core
# POSIX                         - core
# Sys::Hostname                 - core, optional, includes hostname in report if installed
Template                        - required for the HTML report type
Term::Prompt                    - required if password not supplied by configuration
Text::Diff                      - required for Diff, DiffFull, and HTML report types
Text::Diff::HTML                - required for the HTML report type
# Text::Wrap                    - core, optional, some output more tidy if installed
XML::Simple                     - required
