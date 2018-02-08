package FileMgt106::Extraction::Extractor;

=head Copyright licence and disclaimer

Copyright 2011-2018 Franck Latrémolière, Reckon LLP.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

use strict;
use warnings;
use Encode qw(decode_utf8);
use FileMgt106::Database;
use FileMgt106::FileSystem;

sub makeExtractAcceptor {
    my ($sort)    = grep { /^-+sort/i } @_;
    my ($tarpipe) = grep { /^-+(?:tar|tbz|tgz)/i } @_;
    my $fileHandle = \*STDOUT;
    binmode $fileHandle, ':utf8';
    if ( $sort || $tarpipe ) {
        my @list;
        if ($tarpipe) {
            my $options = '';
            if ( $tarpipe =~ /bz/i ) {
                $options = '--bzip2';
            }
            elsif ( $tarpipe =~ /z/i ) {
                $options = '-z';
            }
            if ( my ($cutOffDate) = map { /^-+newer=(.+)/i ? $1 : (); } @_ ) {
                $options .= " --newer-mtime='$cutOffDate'";
            }
            $ENV{COPY_EXTENDED_ATTRIBUTES_DISABLE} = 1;
            $ENV{COPYFILE_DISABLE}                 = 1;
            open $fileHandle, "| tar $options -c -f - -T -";
            binmode $fileHandle, ':utf8';
        }
        return sub {
            unless ( defined $_[0] ) {
                print map { "$_->[0]\n" }
                  sort    { $a->[1] <=> $b->[1] } @list;
                @list = ();
                return;
            }
            my $size = -s $_[0];
            push @list, [ $_[0], $size ];
            return;
        };
    }
    sub {
        print "@_\n" if @_;
        return;
    };
}

sub makeSimpleExtractor {
    my ($acceptor) = @_;
    my $readScal;
    $readScal = sub {
        return $acceptor->()
          unless my ( $what, $path ) = @_;
        $path ||= '.';
        return unless $what;
        if ( ref $what eq 'HASH' ) {
            while ( my ( $k, $v ) = each %$what ) {
                $readScal->( $v, "$path/$k" );
            }
        }
        $acceptor->($path) unless ref $what;
    };
}

sub makeHintsExtractor {

    my ( $hintsFile, $acceptor ) = @_;
    my ( $hints, $searchSha1, $needsNap );

    my $devNo = ( stat $hintsFile )[STAT_DEV];
    require Digest::SHA;
    my $sha1Machine = new Digest::SHA;

    my %done;
    my $processScal;
    $processScal = sub {
        my ($what) = @_;
        if ( $needsNap || !defined $what ) {
            undef $needsNap;
            undef $searchSha1;
            $hints->disconnect if $hints;
            undef $hints;
        }
        return $acceptor->() unless defined $what;
        unless ($searchSha1) {
            $hints = FileMgt106::Database->new( $hintsFile, 1 );
            $hints->{initRootidFromDev}->();
            $searchSha1 = $hints->{searchSha1};
        }
        if ( ref $what eq 'HASH' ) {
            my %h2;
            while ( my ( $k, $v ) = each %$what ) {
                $v = $processScal->($v);
                $h2{$k} = $v if $v;
            }
            return keys %h2 ? \%h2 : undef;
        }
        my $key = lc $what;
        if ( exists $done{$key} ) {
            return $done{$key} ? () : $what;
        }

        my $found;
        my $sha1 = pack( 'H*', $what );
        my $iterator = $searchSha1->( $sha1, $devNo );
        my @candidates;
        while ( my ( $path, $statref, $locid ) = $iterator->() ) {
            next unless -f _ && -r _;
            return $acceptor->( $done{$key} = $path )
              if $locid
              && !( $statref->[STAT_MODE] & 022 )
              && !( $statref->[STAT_UID] && ( $statref->[STAT_MODE] & 0200 ) );
            push @candidates, $path;
        }
        foreach (@candidates) {
            return $acceptor->( $done{$key} = $_ )
              if $sha1 eq eval {
                open my $f, '<', $_;
                $sha1Machine->addfile($f)->digest;
              };
        }
        unless ( ref $what ) {
            undef $done{$key};
            return $what;
        }
    };

}

sub makeDataExtractor {

    my ( $hintsFile, $fileWriter, $resultsProcessor ) = @_;

    require POSIX;
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    my $writer;

    if ($resultsProcessor) {
        $writer = $resultsProcessor->($fileWriter);
    }
    else {
        $fileWriter->(qw(sha1 mtime size ext file folder path rootid inode));
        $writer = sub {
            return $fileWriter->() unless @_;
            my (
                $sha1,   $mtime, $size,   $ext, $name,
                $folder, $row,   $rootid, $inode
            ) = @_;
            $fileWriter->(
                $sha1, [$mtime], [$size], $ext, $name, $folder,
                [qq%="'"&F$row&"/"&E$row&"'"%],
                $rootid, $inode
            );
        };
    }

    my $pathFinder = $hints->{pathFinderFactory}->();

    sub {
        my ($csvOptions) = @_;
        my $q =
          $hints->{dbHandle}->prepare(
                'select rootid, ino, size, name, parid, hex(sha1), mtime'
              . ' from locations where sha1 is not null and size is not null'
              . ( $csvOptions =~ /%/ ? ' and name like ?' : '' )
              . ' order by mtime desc, size desc, sha1, rootid, ino' );
        $q->execute( $csvOptions =~ /%/ ? $csvOptions : () );
        my $row = 1;
        while ( my ( $rootid, $inode, $size, $name, $parid, $sha1, $mtime ) =
            $q->fetchrow_array )
        {
            my ($ext) = ( $name =~ /(\.[a-z0-9-_]+)$/i );
            $ext = '' unless defined $ext;
            next unless defined $parid;
            next unless my $folder = $pathFinder->($parid);
            $mtime = POSIX::strftime( '%Y-%m-%d %H:%M:%S', gmtime $mtime );
            $writer->(
                $sha1,   $mtime, $size,   $ext, $name,
                $folder, ++$row, $rootid, $inode
            );
        }
        $writer->();
    };

}

sub makeInfoExtractor {
    my ($hintsFile) = @_;
    binmode STDOUT, ':utf8';
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    $hints->{initRootidFromDev}->();
    my $devNo       = ( stat $hintsFile )[STAT_DEV];
    my $search      = $hints->{searchSha1};
    my $processScal = sub {
        my ( $sha1hex, $suppressHeader ) = @_;
        return              unless $sha1hex;
        die 'Not supported' unless $sha1hex =~ /^([0-9a-fA-F]{40})$/s;
        print "$sha1hex\n"  unless $suppressHeader;
        my $iterator = $search->( pack( 'H*', $sha1hex ), $devNo );
        my ( @trusted, @untrusted );
        while ( my ( $path, $statref, $locid ) = $iterator->() ) {
            next unless -f _;
            if (
                !$locid
                || ( $statref->[STAT_UID]
                    && ( $statref->[STAT_MODE] & 0200 ) )
                || ( $statref->[STAT_MODE] & 022 )
              )
            {
                push @untrusted, $path;
            }
            else { push @trusted, $path; }
        }
        print join "\n", ( sort @trusted ),
          ( map { " $_"; } sort @untrusted ),
          '';
    };

    my $processQuery = sub {
        local $_ = $_[0];
        my $where = ' ';
        my @args  = ($_);
        if (/^%%$/) {
            $where = ' where sha1 is not null ';
            @args  = ();
        }
        elsif (/%/) {
            $where = ' where name like ? ';
        }
        else {
            $where = ' where name=? ';
        }
        my $q =
          $hints->{dbHandle}
          ->prepare( 'select size, hex(sha1) from locations'
              . $where
              . 'group by size, sha1 order by size desc, sha1 limit 40' );
        $q->execute(@args);
        while ( my ( $size, $sha1hex ) = $q->fetchrow_array ) {
            if ( !defined $size ) {
                $size = 'NULL';
            }
            elsif ( $size > 994_999_999 ) {
                $size = 0.01 * int( 0.5 + $size * 1e-7 ) . 'G';
            }
            elsif ( $size > 994_999 ) {
                $size = 0.01 * int( 0.5 + $size * 1e-4 ) . 'M';
            }
            elsif ( $size > 994 ) {
                $size = 0.01 * int( 0.5 + $size * 0.1 ) . 'k';
            }
            else {
                $size .= ' bytes';
            }
            print "$size $sha1hex\n";
            $processScal->( $sha1hex, 1 );
        }
    };

    $processScal, $processQuery;

}

1;
