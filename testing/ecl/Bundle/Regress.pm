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
