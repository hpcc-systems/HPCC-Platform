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

package Regress::Query;

=pod

=head1 NAME

Regress::Query - perl module used by runregress to provide information about queries

=head1 SYNOPSIS

my $query = Regress::Query->new($path, $engine);

my $skips = $query->skip($opts);

my @variants = $query->setup_variants($setup_clusters, $setup_cluster_types);

my @variants = $query->test_variants($setup_clusters, $setup_cluster_types);

my $skips = $query->skip_variant($opts, $variant);

Regress::Query->print_variant_ecl($variant, $fh);

=cut

use strict;
use warnings;
use Regress::NormalizeFP qw();
use Exporter;
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item Regress::Query->new($path);

Takes the path to an ECL query, and a L<Regress::Engine> object. Returns an object providing information about the query.

=item $query->{path}

Stores the path to the query.

=item $query->{filename}

Stores the filename portion of the path to the query.

=item $query->{xmlname}

Stores the filename with the extension changed to .xml for output.

=cut

sub new($$)
{
    my ($class, $path, $engine) = @_;
    my $self = {path => $path,
                engine => $engine,
                class => '',
                skips => [],
                varskips => [],
                usf => undef,
                uts => undef,
                usi => undef};
    my (undef, undef, $filename) = File::Spec->splitpath($path);
    $self->{filename} = $filename;
    $filename =~ s/\.ecl$/.xml/i;
    $self->{xmlname} = $filename;
    $filename =~ s/\.xml$/.queryxml/i;
    $self->{queryxmlname} = $filename;
    bless($self, $class);
    $self->_read_comments($path);
    return $self;
}

=pod

=item $query->skip($opts);

Takes a L<Regress::Engine> object. If this query should be skipped for the options given, returns a hash where C<reason> is string describing why and C<tbd> indicates whether this skip was flagged TBD; otherwise returns a false value.

=cut

sub skip($$)
{
    my ($self, $opts) = @_;
    return({text => "file class '$self->{class}' does not match suite class '$opts->{class}'"}) if($self->{class} ne $opts->{class});
    foreach my $skip (@{$self->{skips}})
    {
        next unless($self->_matches_skip($skip, %$opts));
        my $reason = $self->_describe_skip($skip);
        return ({text => "query skipped for $reason", tbd => $skip->{tbd}})
    }
    return(undef);
}

=pod

=item $query->setup_variants($setup_clusters, $setup_cluster_types);

Takes a reference to list of clusters, and a reference to a hash of their types. Returns a list of the variants for a setup query. Each variant is a hash: the value of the B<name> key names the variant; the value of the B<cluster> key gives the cluster to run on; the other values are used by L<print_variant_ecl>.

=cut

sub setup_variants($$$)
{
    my ($self, $setup_clusters, $setup_cluster_types) = @_;
    my @variants;
    foreach my $cluster (@$setup_clusters)
    {
        my $type = $setup_cluster_types->{$cluster};
        push(@variants, {name => $cluster, cluster => $cluster});
        push(@variants, {name => join('_', $cluster, 'local'), cluster => $cluster, useLocal => 1}) unless($type eq 'hthor');
        push(@variants, {name => join('_', $cluster, 'payload'), cluster => $cluster, usePayload => 1});
        push(@variants, {name => join('_', $cluster, 'varload'), cluster => $cluster, usePayload => 1, useVarIndex => 1});
    }
    return @variants;
}

=pod

=item $query->test_variants($setup_clusters, $setup_cluster_types);

Takes a reference to a list of setup clusters, and a reference to a hash of their types. If this query requires multiple variants, returns a list of possible variants (which may later be skipped, see below). Otherwise returns an empty list. The variants are as L<setup_variants> above, except that the key C<needsRLT> indicates that RLT should be enabled.

=cut

sub test_variants($$$)
{
    my ($self, $setup_clusters, $setup_cluster_types) = @_;
    return () unless($self->{usf});
    my $variants = [];
    foreach my $cluster (@$setup_clusters)
    {
        my $setuptype = $setup_cluster_types->{$cluster};
        foreach my $useDynamic (0..1)
        {
            foreach my $useLayoutTrans (0..1)
            {
                last if($useLayoutTrans && !$self->{usi});
                $self->_push_variants_for_options($variants, $cluster, $useDynamic, $useLayoutTrans, $setuptype);
            }
        }
    }
    return @$variants;
}

=pod

=item $query->skip_variant($opts, $variant);

Takes a L<Regress::Engine> object and a variant (as above). If this variant of this query should be skipped for the options given, returns a hash where C<reason> is a string describing why and C<tbd> indicates whether this skip was flagged TBD; otherwise returns a false value.

=cut

sub skip_variant($$$)
{
    my ($self, $opts, $variant) = @_;
    return({text => "trans skipped for thor", tbd => 0}) if($variant->{useLayoutTrans} && (($opts->{type} eq 'thor')||($opts->{type} eq 'thorlcr')));
    return({text => "dynamic skipped for non-roxie", tbd => 0}) if($variant->{useDynamic} && ($opts->{type} ne 'roxie'));
    return({text => "local && trans skipped", tbd => 0}) if($variant->{useLayoutTrans} && ($variant->{useLocal}));
    foreach my $skip (@{$self->{varskips}})
    {
        next unless($self->_matches_skip($skip, local => $variant->{useLocal}, payload => $variant->{usePayload}, varload => $variant->{useVarIndex}, trans => $variant->{useLayoutTrans}, dynamic => $variant->{useDynamic}, setuptype => $variant->{setuptype}, %$opts));
        my $reason = $self->_describe_skip($skip);
        return ({text => "query skipped for $reason", tbd => $skip->{tbd}}) 
    }
    return(undef);
}

=pod

=item Regress::Query->print_variant_ecl($variant, $fh);

Takes a variant (as above) and a filehandle. Prints ECL defining a preamble for this variant to the filehandle.

=cut

sub print_variant_ecl($$)
{
    my ($variant, $fh) = @_;
    my $prefix = $variant->{prefix} || $variant->{name};
    print($fh "prefix := '$prefix';\n");
    print($fh "useLayoutTrans := " . ($variant->{useLayoutTrans} ? 'true' : 'false') . ";\n");
    print($fh "useLocal := " . ($variant->{useLocal} ? 'true' : 'false') . ";\n");
    print($fh "usePayload := " . ($variant->{usePayload} ? 'true' : 'false') . ";\n");
    print($fh "useVarIndex := " . ($variant->{useVarIndex} ? 'true' : 'false') . ";\n");
    print($fh "useDynamic := " . ($variant->{useDynamic} ? 'true' : 'false') . ";\n");
}

# PRIVATE

sub _read_comments($$)
{
    my ($self, $path) = @_;
    open(IN, '<', $path) or $self->{engine}->error("Could not read $path: $!");
    while(<IN>)
    {
        my $tbd = s/TBD\s*$//i;
        $self->{class} = lc($1) if(/^\/\/class=(.+?)\s*$/i);
        push(@{$self->{skips}}, {op => 'match', name => 'type', value => lc($1), tbd => $tbd}) if(/^\/\/no(thor|thorlcr|hthor|roxie)($|[ \t])/i);
        push(@{$self->{skips}}, {op => 'match', name => 'os', value => lc($1), tbd => $tbd}) if(/^\/\/no(linux|windows)/i);
        push(@{$self->{varskips}}, {op => 'true', name => 'local', tbd => $tbd}) if(/^\/\/nolocal/i);
        push(@{$self->{skips}}, {multi => $self->_parse_skip($1, $_), tbd => $tbd}) if(/^\/\/skip(.+)/i);
        push(@{$self->{varskips}}, {multi => $self->_parse_skip($1, $_), tbd => $tbd}) if(/^\/\/varskip(.+)/i);
        $self->{usf} = 1 if(/^\/\/UseStandardFiles/i);
        $self->{uts} = 1 if(/^\/\/UseTextSearch/i);
        $self->{usi} = 1 if(/^\/\/UseIndexes/i);
        $self->{needsRLT} = 1 if(/^\/\/needsRLT/i);
    }
    close(IN);
}

sub _parse_skip($$$)
{
    my ($self, $desc, $line) = @_;
    my $parts = [];
    foreach my $item (split(/&&/, $desc))
    {
        if($item =~ /^\s*([_[:alnum:]]+)\s*==\s*([_[:alnum:]]+)\s*$/)
        {
            push(@$parts, {op => 'match', name => lc($1), value => lc($2)});
        }
        elsif($item =~ /^\s*([_[:alnum:]]+)\s*!=\s*([_[:alnum:]]+)\s*$/)
        {
            push(@$parts, {op => 'nomatch', name => lc($1), value => lc($2)});
        }
        elsif($item =~ /^\s*([_[:alnum:]]+)\s*$/)
        {
            push(@$parts, {op => 'true', name => lc($1)});
        }
        elsif($item =~ /^\s*!\s*([_[:alnum:]]+)\s*$/)
        {
            push(@$parts, {op => 'false', name => lc($1)});
        }
        else
        {
            $self->{engine}->error("The query at $self->{path} contained an illegal skip comment: did not recognise the expression '$item' in the line '$line'");
        }
    }
    return $parts;
}

sub _describe_skip($$)
{
    my ($self, $skip) = @_;
    my $multi = $skip->{multi};
    if($multi)
    {
        my @items = map($self->_describe_skip_item($_), @$multi);
        return join(' && ', @items);
    }
    else
    {
        return $self->_describe_skip_item($skip);
    }
}

sub _describe_skip_item($$)
{
    my ($self, $item) = @_;
    if($item->{op} eq 'match')
    {
        return "$item->{name} == $item->{value}";
    }
    elsif($item->{op} eq 'nomatch')
    {
        return "$item->{name} != $item->{value}";
    }
    elsif($item->{op} eq 'true')
    {
        return "$item->{name}";
    }
    elsif($item->{op} eq 'false')
    {
        return "!$item->{name}";
    }
}

sub _matches_skip($$%)
{
    my ($self, $skip, %vars) = @_;
    my $multi = $skip->{multi};
    if($multi)
    {
        foreach my $item (@$multi)
        {
            return 0 unless $self->_matches_skip_item($item, %vars);
        }
        return 1;
    }
    else
    {
        return $self->_matches_skip_item($skip, %vars);
    }
}

sub _matches_skip_item($$%)
{
    my ($self, $item, %vars) = @_;
    my $op = $item->{op};
    my $name = $item->{name};
    my $value = $item->{value};
    if($op eq 'match')
    {
        return 0 unless($vars{$name} && (lc($vars{$name}) eq $value));
    }
    elsif($op eq 'nomatch')
    {
        return 0 if($vars{$name} && (lc($vars{$name}) eq $value));
    }
    if($op eq 'true')
    {
        return 0 unless($vars{$name});
    }
    if($op eq 'false')
    {
        return 0 if($vars{$name});
    }
    return 1;
}

sub _push_variant($$$$$$%)
{
    my ($self, $variants, $cluster, $setuptype, $namemods, $prefixmods, %options) = @_;
    my $name = (@$namemods ? join('_', $cluster, join('', @$namemods)) : $cluster);
    my $prefix = (@$prefixmods ? join('_', $cluster, join('', @$prefixmods)) : $cluster);
    my $needsRLT = ($self->{needsRLT} || $options{useLayoutTrans});
    my $uts = ($self->{uts});
    push(@$variants, {name => $name, prefix => $prefix, setuptype => $setuptype, needsRLT => $needsRLT, uts => $uts, %options});
}

sub _push_variants_for_options($$$$$$)
{
    my ($self, $variants, $cluster, $useDynamic, $useLayoutTrans, $setuptype) = @_;
    my @namemods;
    push(@namemods, 'dynamic') if($useDynamic);
    push(@namemods, 'trans') if($useLayoutTrans);
    $self->_push_variant($variants, $cluster, $setuptype, [@namemods], [], useDynamic => $useDynamic, useLayoutTrans => $useLayoutTrans);
    return unless($self->{usi});
    $self->_push_variant($variants, $cluster, $setuptype, [@namemods, 'local'], ['local'], useLocal => 1, useDynamic => $useDynamic, useLayoutTrans => $useLayoutTrans) unless($setuptype eq 'hthor');
    $self->_push_variant($variants, $cluster, $setuptype, [@namemods, 'payload'], ['payload'], usePayload => 1, useDynamic => $useDynamic, useLayoutTrans => $useLayoutTrans);
    $self->_push_variant($variants, $cluster, $setuptype, [@namemods, 'varload'], ['varload'], usePayload => 1, useVarIndex => 1, useDynamic => $useDynamic, useLayoutTrans => $useLayoutTrans);
}

=pod

=back

=cut

1;
