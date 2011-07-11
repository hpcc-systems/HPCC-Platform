/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
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

package IndexMetadata;

use strict;
use warnings;
use Fcntl qw(SEEK_SET);
use File::stat;
use Math::BigInt;

use Exporter;
our @ISA = qw(Exporter);

# private functions

sub _unpackint($ )
{
    my ($data) = @_;
    my @bytes = split(//, $data);
    my $val = new Math::BigInt('0');
    $val = ($val<<8)+ord($_) foreach (@bytes);
    return $val;
}

sub _packint($$ )
{
    my ($val, $len) = @_;
    my @bytes;
    for (1..$len)
    {
        unshift(@bytes, chr($val % 256));
        $val = $val>>8;
    }
    return join('', @bytes);
}

# private methods

sub _readint($$$ )
{
    my ($self, $offset, $len) = @_;
    seek($self->{fh}, $offset, SEEK_SET);
    my $data;
    my $got = read($self->{fh}, $data, $len);
    die("Corrupt or unreadable index (expected $len-byte integer at offset $offset, read $got)") unless($got == $len);
    return _unpackint($data);
}

my $blank = new Math::BigInt('0xFFFFFFFFFFFFFFFF');

sub _walkpages($ )
{
    my ($self) = @_;
    my $page = $self->_readint(0xF8, 8);
    return if($page == $blank);
    while($page)
    {
        push(@{$self->{pages}}, $page);
        $page = $self->_readint($page, 8);
    }
}

sub _readpage($$ )
{
    my ($self, $offset) = @_;
    my $len = $self->_readint($offset+0x1A, 2);
    my $body;
    my $got = read($self->{fh}, $body, $len);
    die("Corrupt or unreadable index (page at offset $offset indicated length $len, read $got)") unless($got == $len);
    return $body;
}

sub _resetpages($ )
{
    my ($self) = @_;
    my @oldpages = sort({$b <=> $a} @{$self->{pages}}); # @oldpages is in reverse numerical order
    $self->{pages} = [];
    return @oldpages;
}

sub _nextpage($$ )
{
    my ($self, $oldpages) = @_;
    my $nextpage = pop(@$oldpages); # @$oldpages is in reverse numerical order, so use pop to fill from start
    unless($nextpage)
    {
        $nextpage = $self->{size};
        $self->{size} += 8192;
    }
    push(@{$self->{pages}}, $nextpage);
    return $nextpage;
}

sub _writepage($$$$$$ )
{
    my ($self, $xml, $prevpage, $page, $more, $oldpages) = @_;
    my @pages = @{$self->{pages}};
    my $nextpage = $more ? $self->_nextpage($oldpages) : 0;
    my $len = length($xml);
    my $fh = $self->{fh};
    seek($fh, $page, SEEK_SET);
    print($fh _packint($nextpage, 8));
    print($fh _packint($prevpage, 8));
    print($fh _packint(3, 10));
    print($fh _packint($len, 2));
    $xml .= (chr(0) x (8164-$len));
    print($fh $xml);
    return $nextpage;
}

sub _fixhead($ )
{
    my ($self) = @_;
    my $fh = $self->{fh};
    my $page = @{$self->{pages}} ? $self->{pages}->[0] : 0;
    seek($fh, 0xF8, SEEK_SET);
    print($fh _packint($page, 8));
}

sub _clearpages($@ )
{
    my ($self, $oldpages) = @_;
    return unless(@$oldpages);
    my $newsize = $self->{size};
    foreach my $page (@$oldpages) # @$oldpages is in reverse numerical order, so use foreach to remove unused from end
    {
        if($page = $newsize - 8192)
        {
            $newsize = $page;
        }
        else
        {
            my $fh = $self->{fh};
            seek($fh, $page, SEEK_SET);
            print($fh (chr(0) x 8192));
            warn("Metadata shortened or removed but unused page at $page is not at end of file, blanking instead");
        }
    }
    if($newsize < $self->{size})
    {
        truncate($self->{fh}, $newsize) or die("Could not trunctate filehandle to $newsize bytes");
        $self->{size} = $newsize;
    }
}

# public methods

sub new($$ )
{
    my ($class, $fh) = @_;
    my $self = {fh => $fh, size => 0, pages => []};
    bless($self, $class);
    my $stat = stat($fh) or die("Could not stat filehandle");
    $self->{size} = $stat->size;
    $self->_walkpages();
    return $self;
}

sub read($ )
{
    my ($self) = @_;
    return undef unless(@{$self->{pages}});
    my $xml;
    $xml .= $self->_readpage($_) foreach (@{$self->{pages}});
    return $xml;
}

sub rewrite($$ )
{
    my ($self, $xml) = @_;
    die("Attempt to write empty metadata (use strip to remove)") unless($xml);
    my @oldpages = $self->_resetpages();

    my $written = 0;
    my $len = length($xml);
    my $prevpage = 0;
    my $page = $self->_nextpage(\@oldpages);
    while($page)
    {
        my $chunk = substr($xml, $written, 8164);
        $written += length($chunk);
        my $nextpage = $self->_writepage($chunk, $prevpage, $page, ($written < $len), \@oldpages);
        $prevpage = $page;
        $page = $nextpage;
    }

    $self->_fixhead();
    $self->_clearpages(\@oldpages);
}

sub strip($ )
{
    my ($self) = @_;
    my @oldpages = $self->_resetpages();
    $self->_fixhead();
    $self->_clearpages(\@oldpages);
}

# public functions

sub readfile($ )
{
    my ($filename) = @_;
    open(FH, '<:bytes', $filename) or die("could not open $filename for reading: $!");
    my $meta = IndexMetadata->new(*FH);
    my $xml = $meta->read();
    close(FH);
    return $xml;
}

sub stripfile($ )
{
    my ($filename) = @_;
    open(FH, '+<:bytes', $filename) or die("could not open $filename for rewriting: $!");
    my $meta = IndexMetadata->new(*FH);
    $meta->strip();
    close(FH);
}

1;

__END__

=head1 NAME

IndexMetadata - perl module to read, rewrite, or strip the metadata from physical index parts

=head1 SYNOPSIS

To read and print metadata:

  open(FH, '<:bytes', 'myindex') or die("could not open myindex for reading: $!");
  my $meta = IndexMetadata->new(*FH);
  my $xml = $meta->read();
  close(FH);
  print($xml);

...or simply:

  print(IndexMetadata::readfile('myindex'));

To modify metadata:

  open(FH, '+<:bytes', 'myindex') or die("could not open myindex for rewriting: $!");
  my $meta = IndexMetadata->new(*FH);
  my $xml = $meta->read();
  # ...
  $meta->rewrite($new_xml);
  close(FH);

To strip out metadata:

  open(FH, '+<:bytes', 'myindex') or die("could not open myindex for rewriting: $!");
  my $meta = IndexMetadata->new(*FH);
  $meta->strip();
  close(FH);

...or simply:

  IndexMetadata::stripfile('myindex');

=head1 DESCRIPTION

=head2 METHODS

=over

=item C<$meta = IndexMetadata-E<gt>new($fh)>

Returns a new metadata handler object for the index at the given filehandle reference. It is an error if it is not possible to stat and read the filehandle, or if it does not point to a valid index.

=item C<$xml = $meta-E<gt>read()>

Returns the index metadata, as a string. It is expected that this string should parse as well-formed XML with a document root named C<metadata>. Returns C<undef> if the index has no metadata. It is an error the index is corrupt or unreadable.

=item C<$meta-E<gt>rewrite($xml)>

Rewrites the index metadata. It is an error if the metadata is empty, or if the index is unwritable, or untruncatable where required (see below). The metadata is taken as a string. B<IMPORTANT>: It is the caller's responsibility to ensure that this string parses as well-formed XML with a document root named C<metadata>.

=item C<$meta-E<gt>strip()>

Strips all metadata from the index. It is an error if the index is unwritable, or untruncatable where required (see below).

=back

=head2 HELPER FUNCTIONS

=over

=item C<$xml = IndexMetadata::readfile($filename)>

Returns the metadata from the named file.

=item C<IndexMetadata::stripfile($filename)>

Strips the metadata from the named file.

=back

=head2 NOTE ON TRUNCATION

If an index contains metadata which is replaced with a string sufficiently shorter using C<rewrite> (specifically one which divides into less 8164 byte chunks) or removed using C<strip> then 8192 byte pages will be removed from the file. Normally, these unused metadata pages will be at the end of the index, and the filehandle will simply be truncated. If for some reason the pages do not fall at the end, they will be blanked instead, as reordering non-metadata pages is not easily possible: warnings will be issued when this occurs.
