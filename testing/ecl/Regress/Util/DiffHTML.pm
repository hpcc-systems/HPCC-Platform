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

package Regress::Util::DiffHTML;

=pod

=head1 NAME
    
Regress::Util::DiffHTML - perl module used by runregress, subclasses Text::Diff::HTML as a drop-in replacement which puts del/ins tags around the filename header lines in the same way as they appear around the corresponding hunk lines

=cut

use strict;
use warnings;
use HTML::Entities;
use Text::Diff::HTML ();

our @ISA = qw(Text::Diff::HTML);

sub _header($ )
{
    my ($opts) = @_;
    my ($p1, $s1, $fn1, $t1, $p2, $s2, $fn2, $t2) = @{$opts}{"FILENAME_PREFIX_A",
                                                             "FILENAME_SUFFIX_A",
                                                             "FILENAME_A",
                                                             "MTIME_A",
                                                             "FILENAME_PREFIX_B",
                                                             "FILENAME_SUFFIX_B",
                                                             "FILENAME_B",
                                                             "MTIME_B"};

    return("") unless(defined($fn1) && defined($fn2));

    return join("",
                $p1, " ", $fn1, (defined($t1) ? "\t" . localtime($t1) : ()), (defined $s1 ? $s1 : ()), "\n",
                $p2, " ", $fn2, (defined($t2) ? "\t" . localtime($t2) : ()), (defined $s2 ? $s2 : ()), "\n");

}

sub file_header($$ )
{
    my $self = shift();
    my $options = pop();
    my %encopts;
    $encopts{$_} = encode_entities($options->{$_}) foreach(keys(%$options));
    return('<div class="file"><span class="fileheader">'
           . _header({FILENAME_PREFIX_A => '<del>---',
                      FILENAME_SUFFIX_A => '</del>',
                      FILENAME_PREFIX_B => '<ins>+++',
                      FILENAME_SUFFIX_B => '</ins>',
                      %$options})
           . '</span>');
}

=pod

=head1 DESCRIPTION

This is a drop-in replacement for the module L<Text::Diff::HTML>. It differs in two ways. Firstly, the C<div> elements are replaced by C<span>s, because you will normally want to enclose the whole lot in a C<pre> element and C<div> is not allowed inside C<pre>. Secondly, it puts C<ins> and C<del> elements around the two filenames in the fileheader, so that they can be coloured etc. in the same way as the corresponding lines in the diff.

=cut

1;
