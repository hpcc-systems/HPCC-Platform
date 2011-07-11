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

package Regress::Prepare;

=pod

=head1 NAME
    
Regress::Prepare - perl module used by runregress to prepare a regression suite

=head1 SYNOPSIS

my $submit = Regress::Prepare->new($engine);

$prepare->prepare();

my $saved_config = $prepare->load_settings();

my $queries = $prepare->list_queries();

=cut
    
use strict;
use warnings;
use Config::Simple qw();
use File::Spec::Functions qw(catdir catfile splitpath);
use File::Path qw(mkpath);
use Regress::Query qw();
use Exporter;
our @ISA = qw(Exporter);

=pod

=head1 DESCRIPTION

=over

=cut

# PUBLIC

=pod

=item my $prepare = Regress::Prepare->new($engine);

Takes a L<Regress::Engine> object. Returns an object used to prepare a regression suite.

=cut

sub new($$)
{
    my ($class, $engine) = @_;
    my $self = {engine => $engine,
                $engine->options()};
    $self->{setuptxt} = catfile($self->{testdir}, 'setup', 'setup.txt');
    $self->{textsearchtxt} = catfile($self->{testdir}, 'setup', 'textsearch.txt');
    return bless($self, $class);
}

=pod

=item $prepare->prepare();

Prepares the regression suite. Records a list of queries to be run in its engine.

=cut

sub prepare($)
{
    my ($self) = @_;
    if($self->{setup_generate})
    {
        $self->_prepare_setup();
    }
    else
    {
        $self->_prepare_test();
    }
    $self->_record_manifest();
    $self->_record_skipped('skipped', 'Skipped');
    $self->_record_skipped('tbd', 'TBD');
    $self->_save_settings();
}

=pod

=item my $saved_config = $prepare->load_settings();

Loads the descriptive settings saved at the end of L<prepare> and returns them as a hash ref.

=cut

sub load_settings($)
{
    my ($self) = @_;
    my $cfgreader = new Config::Simple();
    my $cfgpath = File::Spec->catfile($self->{suite}, 'settings.ini');
    $cfgreader->read($cfgpath) or $self->{engine}->error("Could not read $cfgpath: " . $cfgreader->error());
    my $cfg = $cfgreader->vars();
    return $cfg;
}

=pod

=item my $queries = $prepare->list_queries();

Returns a list of the queries to be run, if the C<query> option is set, else return undef (indicating all queries to be run).

=cut

sub list_queries($)
{
    my ($self) = @_;
    return undef unless($self->{query});
    my $counts = {};
    $self->_paths_to_queries($counts, map($self->_query_to_paths(undef, $_), @{$self->{query}}));
    $self->_paths_to_queries($counts, map($self->_query_to_paths($self->{os}, $_), @{$self->{query}})) if($self->{os});
    $self->_paths_to_queries($counts, map($self->_query_to_paths($self->{type}, $_), @{$self->{query}})) if($self->{type});
    my @queries = keys(%$counts);
    return \@queries;
}

# PRIVATE

sub _query_to_paths($$$)
{
    my ($self, $subdir, $query) = @_;
    $query = "$query.ecl" unless($query =~ /\.ecl$/i);
    my $pattern = catfile($self->{testdir}, $subdir, $query);
    return glob($pattern);
}

sub _paths_to_queries($@)
{
    my ($self, $counts, @paths) = @_;
    foreach my $path(@paths)
    {
        next unless(-e $path);
        my (undef, undef, $query) = splitpath($path);
        ++$counts->{$query};
    }
}

sub _prepare_setup($)
{
    my ($self) = @_;

    mkpath(catdir($self->{suite}, 'setup'));
    my $srcpattern = catfile($self->{testdir}, 'setup', '*.ecl');
    $self->_prepare_setup_query($_) foreach (glob($srcpattern));
}

sub _prepare_setup_query($$)
{
    my ($self, $path) = @_;
    my $query = Regress::Query->new($path, $self->{engine});
    $self->{engine}->error("Query name $query->{filename} contains illegal characters") if($query->{filename} =~ /[^[:alnum:,-]_\.]/);
    my @variants = $query->setup_variants($self->{setup_clusters}, $self->{setup_cluster_types});
    foreach my $variant (@variants)
    {
        my $destname = join('_', $variant->{name}, $query->{filename});
        my $destpath = catfile($self->{suite}, 'setup', $destname);
        $self->_copy_query($query->{path}, $destpath, $variant);
        my $destxml = join('_', $variant->{name}, $query->{xmlname});
        my $outpath = catfile($self->{suite}, 'setup', $destxml);
        my $run = {query => $query->{filename}, variant => $variant->{name}, path => $destpath, outpath => $outpath, cluster => $variant->{cluster}, queryxmlpath=>''};
        $self->{engine}->note_to_run($run);
        push(@{$self->{manifest}->{$query->{filename}}}, {variant => $variant->{name}, outpath => $outpath});
    }
}

sub _prepare_test($)
{
    my ($self) = @_;
    mkpath(catdir($self->{suite}, 'key'));
    mkpath(catdir($self->{suite}, 'out'));
    $self->_prepare_test_dir();
    $self->_prepare_test_dir($self->{os}) if($self->{os});
    $self->_prepare_test_dir($self->{type}) if($self->{type});
}

sub _prepare_test_dir($;$)
{
    my ($self, $subdir) = @_;
    if($self->{query})
    {
        my @paths = map($self->_query_to_paths($subdir, $_), @{$self->{query}});
        $self->_prepare_test_query($subdir, $_) foreach(@paths);
    }
    else
    {
        my $srcpattern = catfile($self->{testdir}, $subdir, '*.ecl');
        $self->_prepare_test_query($subdir, $_) foreach(glob($srcpattern));
    }
}

sub _prepare_test_query($$$)
{
    my ($self, $subdir, $path) = @_;
    return unless(-e $path);
    my $query = Regress::Query->new($path, $self->{engine});
    $self->{engine}->error("Query name $query->{filename} contains illegal characters") if($query->{filename} =~ /[^[:alnum:,-]_\.]/);

    my $skip = $query->skip($self);
    if($skip)
    {
        push(@{$self->{skipped}->{$query->{filename}}}, {reason => $skip->{text}});
        push(@{$self->{tbd}->{$query->{filename}}}, {reason => $skip->{text}}) if($skip->{tbd});
        return;
    }

    my $keysrcpath = $self->_key_srcpath($subdir, $query->{xmlname});
    $self->{engine}->warning("Missing key file $query->{xmlname}") unless(-e $keysrcpath);

    my $queryxmlpath = $query->{queryxmlname};

    my @variants = $query->test_variants($self->{setup_clusters}, $self->{setup_cluster_types});
    if(@variants)
    {
        foreach my $variant (@variants)
        {
            next if($self->{variant} && !grep({$variant->{name} eq $_} @{$self->{variant}}));
            my $skip = $query->skip_variant($self, $variant);
            if($skip)
            {
                push(@{$self->{skipped}->{$query->{filename}}}, {variant => $variant->{name}, reason => $skip->{text}});
                push(@{$self->{tbd}->{$query->{filename}}}, {variant => $variant->{name}, reason => $skip->{text}}) if($skip->{tbd});
            }
            else
            {
                my $destname = join('_', $variant->{name}, $query->{filename});
                my $destpath = catfile($self->{suite}, $destname);
                $self->_copy_query($query->{path}, $destpath, $variant);

                my $destxml = join('_', $variant->{name}, $query->{xmlname});
                my $keydestpath = catfile($self->{suite}, 'key', $destxml);
                $self->{engine}->normal_copy($keysrcpath, $keydestpath, $query->{postfilter});

                my $outpath = catfile($self->{suite}, 'out', $destxml);
                my $run = {query => $query->{filename}, variant => $variant->{name}, path => $destpath, outpath => $outpath, cluster => $self->{cluster}, postfilter => $query->{postfilter}, queryxmlpath => $queryxmlpath};
                $self->{engine}->note_to_run($run);
                push(@{$self->{manifest}->{$query->{filename}}}, {variant => $variant->{name}, outpath => $outpath, keypath => $keydestpath});
            }
        }
    }
    else
    {
        my $destpath = catfile($self->{suite}, $query->{filename});
        $self->_copy_query($query->{path}, $destpath);

        my $keydestpath = catfile($self->{suite}, 'key', $query->{xmlname});
        $self->{engine}->normal_copy($keysrcpath, $keydestpath, $query->{postfilter});

        my $outpath = catfile($self->{suite}, 'out', $query->{xmlname});
        my $run = {query => $query->{filename}, path => $destpath, outpath => $outpath, cluster => $self->{cluster}, postfilter => $query->{postfilter}, queryxmlpath => $queryxmlpath};
        $self->{engine}->note_to_run($run);
        push(@{$self->{manifest}->{$query->{filename}}}, {outpath => $outpath, keypath => $keydestpath});
    }
}

sub _copy_query($$$;$)
{
    my ($self, $srcpath, $destpath, $variant) = @_;
    open(OUT, '>', $destpath) or $self->{engine}->error("Could not write $destpath: $!");
    print(OUT "#option('$_', $self->{ecloptions}->{$_});\n") foreach(sort(keys(%{$self->{ecloptions}})));
    my $needsRLT = ($variant && $variant->{needsRLT} ? 1 : 0);
    print(OUT "#option('layoutTranslationEnabled', $needsRLT);\n");
    Regress::Query::print_variant_ecl($variant, \*OUT) if($variant);
    my $setupFileLocation = "$self->{setup_file_location}";
    print(OUT "setupTextFileLocation := '$setupFileLocation';\n");
    my $usesTextSearch = ($variant && $variant->{uts});
    my @paths;
    push(@paths, $self->{setuptxt}) if($variant);
    push(@paths, $self->{textsearchtxt}) if($usesTextSearch);
    push(@paths, $srcpath);
    foreach my $path (@paths)
    {
        open(IN, '<', $path) or $self->{engine}->error("Could not read $path: $!");
        print(OUT "#line(0)\n");
        print(OUT) while(<IN>);
        close(IN);
    }
    close(OUT);
    return $destpath;
}

sub _key_srcpath($$$)
{
    my ($self, $subdir, $filename) = @_;
    if($subdir)
    {
        my $subdirsrc = catfile($self->{testdir}, $subdir, 'key', $filename);
        return $subdirsrc;
    }
    if($self->{type})
    {
        my $typesrc = catfile($self->{testdir}, $self->{type}, 'key', $filename);
        return $typesrc if(-e $typesrc);
    }
    if($self->{os})
    {
        my $ossrc = catfile($self->{testdir}, $self->{os}, 'key', $filename);
        return $ossrc if(-e $ossrc);
    }
    my $src = catfile($self->{testdir}, 'key', $filename);
    return $src;
}

sub _record_manifest($)
{
    my ($self) = @_;
    my $manifestfile = catfile($self->{suite}, 'manifest.csv');
    open(OUT, '>', $manifestfile) or $self->{engine}->error("Could not write $manifestfile: $!");
    my @queries = keys(%{$self->{manifest}});
    foreach my $query (sort(@queries))
    {
        foreach (@{$self->{manifest}->{$query}})
        {
            print(OUT join(',', $query, $_->{variant} || '', $_->{keypath} || '', $_->{outpath}), "\n");
            if($self->{preview})
            {
                if($_->{variant})
                {
                    $self->{engine}->log_write("Generated: $query / $_->{variant}");
                }
                else
                {
                    $self->{engine}->log_write("Generated: $query");
                }
            }
        }
    }
    close(OUT);
}

sub _record_skipped($$$)
{
    my ($self, $type, $label) = @_;
    my $skippedfile = catfile($self->{suite}, "$type.csv");
    open(OUT, '>', $skippedfile) or $self->{engine}->error("Could not write $skippedfile: $!");
    my @queries = keys(%{$self->{$type}});
    foreach my $query (sort(@queries))
    {
        foreach (@{$self->{$type}->{$query}})
        {
            print(OUT join(',', $query, $_->{variant} || '', $_->{reason}), "\n");
            if($self->{preview})
            {
                if($_->{variant})
                {
                    $self->{engine}->log_write("$label: $query / $_->{variant} ($_->{reason})");
                }
                else
                {
                    $self->{engine}->log_write("$label: $query ($_->{reason})");
                }
            }
        }
    }
    close(OUT);
}

sub _save_settings($)
{
    my ($self) = @_;
    my $cfgwriter = new Config::Simple(syntax => 'simple');
    $cfgwriter->param($_, $self->{desc}->{$_}) foreach(keys(%{$self->{desc}}));
    my $cfgpath = File::Spec->catfile($self->{suite}, 'settings.ini');
    $cfgwriter->write($cfgpath) or $self->{engine}->error("Could not write $cfgpath: " . $cfgwriter->error());
}

=pod

=back

=cut

1;
