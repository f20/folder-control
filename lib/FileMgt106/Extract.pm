package FileMgt106::Extract;

=head Copyright licence and disclaimer

Copyright 2011-2014 Franck Latrémolière, Reckon LLP.

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
use utf8;
use Digest::SHA ();
use FileMgt106::Database;
use FileMgt106::FileSystem;

sub makeExtractAcceptor {
    my ($sort)    = grep { /^-+sort/i } @_;
    my ($tarpipe) = grep { /^-+(?:tar|tbz|tgz)/i } @_;
    if ( $sort || $tarpipe ) {
        my @list;
        my $fileHandle = \*STDOUT;
        if ( my ($tarpipe) = grep { /^-+(?:tar|tbz|tgz)/i } @_ ) {
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
        }
        return sub {
            unless ( defined $_[0] ) {
                print map { "$_->[0]\n" }
                  sort    { $a->[1] <=> $b->[1] } @list;
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
    $SIG{ALRM} = sub {
        $needsNap = 1;
        alarm 15;
    };
    alarm 20;

    my $devNo = ( stat '.' )[STAT_DEV];
    require Digest::SHA;
    my $sha1Machine = new Digest::SHA;

    my %done;
    my $processScal;
    $processScal = sub {
        my ($what) = @_;
        if ( $needsNap || !$what ) {
            undef $needsNap;
            undef $searchSha1;
            $hints->{dbHandle}->disconnect if $hints && $hints->{dbHandle};
            undef $hints;
        }
        return $acceptor->() unless $what;
        unless ($searchSha1) {
            $hints = FileMgt106::Database->new( $hintsFile, 1 );
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

sub _escapeCsv {
    ( local $_ ) = @_;
    s/"/""/g;
    qq%"$_"%;
}

sub makeCsvWriter {
    my ($dumpFile) = @_;
    $dumpFile ||= '';
    $dumpFile =~ s/[^%_\.a-zA-Z0-9]+/_/gs;
    my $c;
    if ($dumpFile) {
        open $c, '>', "$dumpFile.$$";
    }
    else {
        $c = \*STDOUT;
    }
    binmode $c, ':utf8';
    sub {
        if (@_) {
            print {$c} join( ',', map { /[" ]/ ? _escapeCsv($_) : $_; } @_ )
              . "\n";
        }
        else {
            close $c;
            rename "$dumpFile.$$", "$dumpFile.csv" if $dumpFile;
        }
    };
}

sub makeSpreadsheetWriter {
    my ( $format, $fileName ) = @_;
    my $module;
    if ( $format =~ /^xls$/s ) {
        if ( eval { require Spreadsheet::WriteExcel; } ) {
            $fileName .= '.xls';
            $module = 'Spreadsheet::WriteExcel';
        }
        else {
            warn "Could not load Spreadsheet::WriteExcel: $@";
        }
    }
    if ( !$module ) {
        if ( eval { require Excel::Writer::XLSX; } ) {
            $fileName .= '.xlsx';
            $module = 'Excel::Writer::XLSX';
        }
        else {
            warn "Could not load Excel::Writer::XLSX: $@";
        }
    }
    if ( !$module && eval { require Spreadsheet::WriteExcel; } ) {
        $fileName .= '.xls';
        $module = 'Spreadsheet::WriteExcel';
    }
    if ($module) {
        warn "Using $module";
        my $wb = $module->new( $fileName . $$ );
        my $ws = $wb->add_worksheet('Extracted');
        $ws->set_paper(9);
        $ws->fit_to_pages( 1, 0 );
        $ws->hide_gridlines(2);
        my @colWidth = qw(8 24 12 8 48 48 8 12 12);
        $ws->set_column( $_, $_, $colWidth[$_] ) foreach 0 .. $#colWidth;

        my $dateFormat;
        my $wsWrite = sub {
            ( my $ws, my $row, my $col, local $_ ) = @_;
            if (/^([0-9]+-[0-9]+-[0-9]+)[ T]([0-9]+:[0-9]+:[0-9]+)$/s) {
                $ws->write_date_time(
                    $row, $col,
                    $1 . 'T' . $2,
                    $dateFormat ||= $wb->add_format(
                        num_format => 'ddd d mmm yyyy HH:MM:SS'
                    )
                );
            }
            else {
                $ws->write( $row, $col, $_ );
            }
        };

        my $row = -1;
        return sub {

            if (@_) {
                ++$row;
                $wsWrite->( $ws, $row, $_, $_[$_] ) foreach 0 .. $#_;
            }
            else {
                $ws->autofilter( 0, 0, $row, $#colWidth );
                undef $wb;
                rename $fileName . $$, $fileName;
            }
        };
    }
    warn 'Using CSV';
    makeCsvWriter($fileName);
}

sub makeDataExtractor {

    my ( $hintsFile, $writer ) = @_;

    require POSIX;
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );

    $writer->(qw(sha1 mtime size ext file folder path rootid inode));

    my $pathFinder;
    {
        my %paths;
        my $q =
          $hints->{dbHandle}
          ->prepare('select name, parid from locations where locid=?');
        $pathFinder = sub {
            my ($locid) = @_;
            return $paths{$locid} if exists $paths{$locid};
            $q->execute($locid);
            my ( $name, $parid ) = $q->fetchrow_array;
            return $paths{$locid} = undef unless defined $parid;
            return $paths{$locid} = $name unless $parid;
            my $p = $pathFinder->($parid);
            return $paths{$locid} = undef unless defined $p;
            $paths{$locid} = "$p/$name";
        };
    }

    sub { }, sub {
        my ($csvOptions) = @_;
        my $q =
          $hints->{dbHandle}
          ->prepare( 'select rootid, ino, size, name, parid, hex(sha1), mtime'
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
            next unless my $p = $pathFinder->($parid);
            $mtime = POSIX::strftime( '%Y-%m-%d %H:%M:%S', gmtime $mtime );
            ++$row;
            $writer->(
                $sha1, $mtime, $size, $ext, $name, $p,
                qq%="'"&F$row&"/"&E$row&"'"%, $rootid, $inode
            );
        }
        $writer->();
    };

}

sub makeInfoExtractor {
    my ($hintsFile) = @_;
    my $hints       = FileMgt106::Database->new( $hintsFile, 1 );
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
        print join "\n", ( sort @trusted ), ( map { " $_"; } sort @untrusted ),
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

sub makeHintsBuilder {

    my ($hintsFile) = @_;
    my $searchSha1 = FileMgt106::Database->new( $hintsFile, 1 )->{searchSha1};

    my $createTree;
    $createTree = sub {

        my ( $whatYouWant, $whereYouWantIt, $devNo ) = @_;
        unless ($devNo) {
            $whereYouWantIt ||= '.';
            die "No device for $whereYouWantIt"
              unless $devNo = ( stat $whereYouWantIt )[STAT_DEV];
        }

        # To contain a scalar representing missing objects (or false if none).
        my $returnValue;

      ENTRY: while ( my ( $name, $what ) = each %$whatYouWant ) {
            next if $name =~ m#/#;
            my $fileName = "$whereYouWantIt/$name";
            if ( -l $fileName ) {
                unlink $fileName or die "unlink $fileName: $!";
            }
            if ( ref $what ) {
                if ( ref $what eq 'HASH' ) {
                    die "File in the way: $fileName" if -f _;
                    mkdir $fileName;
                    my $rv = $createTree->( $what, $fileName, $devNo );
                    $returnValue->{$name} = $rv if $rv;
                }
                next;
            }
            die "Item in the way: $fileName" if -e _;
            unless ( $what =~ /([0-9a-fA-F]{40})/ ) {
                symlink $what, $fileName or $returnValue->{$name} = $what;
                next;
            }
            my $sha1 = pack( 'H*', $1 );
            my $iterator = $searchSha1->( $sha1, $devNo );
            my ( @stat, @candidates, @reservelist );
            while ( !@stat
                && ( my ( $path, $statref, $locid ) = $iterator->() ) )
            {
                next unless -f _;
                if (
                    !$locid
                    || ( $statref->[STAT_UID]
                        && ( $statref->[STAT_MODE] & 0200 ) )
                    || ( $statref->[STAT_MODE] & 022 )
                  )
                {
                    push @reservelist, $path;
                    next;
                }
                if (   $statref->[STAT_DEV] != $devNo
                    || $statref->[STAT_UID] && $statref->[STAT_UID] < 500 )
                {
                    push @candidates, $path;
                    next;
                }
                next ENTRY if link $path, $fileName;
            }
            foreach ( @candidates, @reservelist ) {
                next ENTRY if _copyFile( $_, $fileName );
            }
            $returnValue->{$name} = $what;

        }

        $returnValue;

    };

}

sub _copyFile {
    my $status = system qw(cp -p --), @_;
    return 1 if 0 == $status;
    warn join ' ', qw(system cp -p --), @_, 'returned',
      unpack( 'H*', pack( 'n', $status ) ), 'Caller:', caller,
      'Cwd:',
      `pwd`;
    return;
}

sub makeHintsFilter {

    my ($hintsFile) = @_;
    my $searchSha1 = FileMgt106::Database->new( $hintsFile, 1 )->{searchSha1};
    my $sha1Machine;

    my $filterTree;
    $filterTree = sub {

        my ( $whatYouWant, $devNo ) = @_;
        unless ($devNo) {
            die "No device for ."
              unless $devNo = ( stat '.' )[STAT_DEV];
        }

        # To contain a scalar representing missing objects (or false if none).
        my $returnValue;

      ENTRY: while ( my ( $name, $what ) = each %$whatYouWant ) {
            next if $name =~ m#/#;
            if ( ref $what ) {
                if ( ref $what eq 'HASH' ) {
                    my $rv = $filterTree->( $what, $devNo );
                    $returnValue->{$name} = $rv if $rv;
                }
                next;
            }
            next unless $what =~ /([0-9a-fA-F]{40})/;
            my $sha1 = pack( 'H*', $1 );
            my $iterator = $searchSha1->( $sha1, $devNo );
            my ( @stat, @candidates, @reservelist );
            while ( !@stat
                && ( my ( $path, $statref, $locid ) = $iterator->() ) )
            {
                next unless -f _;
                if (
                    !$locid
                    || ( $statref->[STAT_UID]
                        && ( $statref->[STAT_MODE] & 0200 ) )
                    || ( $statref->[STAT_MODE] & 022 )
                  )
                {
                    push @reservelist, $path;
                    next;
                }
                next ENTRY;
            }
            if ( @candidates || @reservelist ) {
                unless ($sha1Machine) {
                    require Digest::SHA;
                    $sha1Machine = new Digest::SHA;
                }
                foreach ( @candidates, @reservelist ) {
                    next ENTRY if $sha1 eq $sha1Machine->addfile($_)->digest;
                }
            }

            $returnValue->{$name} = $what;

        }

        $returnValue;

    };

}

1;
