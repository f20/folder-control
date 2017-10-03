package FileMgt106::ScanMasterAperture;

=head Copyright licence and disclaimer

Copyright 2011-2017 Franck Latrémolière.

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
use Cwd;
use Sys::Hostname qw(hostname);
use Encode qw(decode_utf8 encode_utf8);
use FileMgt106::Scanner;
use FileMgt106::FileSystem;

use constant {
    LIB_DIR   => 0,
    LIB_MTIME => 1,
    LIB_JBZ   => 2,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub repairPermissions {
    my ($lib) = @_;
    unless ( chdir $lib->[LIB_DIR] ) {
        warn die "Cannot chdir $lib->[LIB_DIR]";
        return;
    }
    my $rgid = ( stat '.' )[STAT_GID];
    my $repairer;
    $repairer = sub {
        foreach (@_) {
            my @stat = lstat $_;
            if ( -d _ ) {
                chdir $_ or next;
                opendir DIR, '.';
                my @list = grep { !/^\.\.?$/s; } readdir DIR;
                closedir DIR;
                $repairer->(@list);
                chdir '..';
            }
            elsif ( -f _ ) {
                if ( $stat[STAT_NLINK] > 1 ) {
                    system 'cp', '-p', '--', $_,
                      'aperture_repair_permissions_temporary';
                    unless ( rename 'aperture_repair_permissions_temporary',
                        $_ )
                    {
                        warn "Cannot rename to $_ in $lib->[LIB_DIR]";
                        next;
                    }
                    @stat = lstat $_;
                }
                if ( $stat[STAT_NLINK] > 1 ) {
                    warn "$_ still multilinked in $lib->[LIB_DIR]";
                    next;
                }
                chown -1, $rgid, $_ unless $rgid == $stat[STAT_GID];
                chmod 0660, $_ unless $stat[STAT_MODE] & 0660 == 0660;
            }
        }
    };
    opendir DIR, '.';
    my @list = grep { !/^\.\.?|Masters$/s; } readdir DIR;
    closedir DIR;
    $repairer->(@list);
    {
        local $_ = $lib->[LIB_DIR];
        s/\.aplibrary$/ (Masters)/;
        unlink $_;
        symlink "$lib->[LIB_DIR]/Masters", $_;
    }
    $rgid;
}

sub extractApertureMetadata {
    my ($lib) = @_;
    return unless -s "$lib->[LIB_DIR]/Database/apdb/Library.apdb";
    my $lib =
      DBI->connect(
        "dbi:SQLite:dbname=$lib->[LIB_DIR]/Database/apdb/Library.apdb",
        '', '', { sqlite_unicode => 0, AutoCommit => 1, } );
    unless ($lib) {
        warn "Cannot open $lib->[LIB_DIR]/Database/apdb/Library.apdb";
        return;
    }
    my %metadata;
    foreach (
        @{
            $lib->selectall_arrayref(
                    'select uuid, mainRating, isOriginal '
                  . 'masterUuid, rawMasterUuid, nonRawMasterUuid '
                  . ' from RKVersion'
            )
        }
      )
    {
        my ( $v, $stars, $isOriginal, @masters ) = @$_;
        next if !$stars && $isOriginal;
        $stars ||= 0;
        $metadata{starsById}{$v}     = $stars;
        $metadata{masterPrimary}{$v} = $masters[0];
        foreach my $m ( grep { $_ } @masters ) {
            $metadata{starsById}{$m} = $stars
              unless $metadata{starsById}{$m}
              && $metadata{starsById}{$m} > $stars;
            $metadata{masterSecondary}{$v} = $m
              unless defined $masters[0] && $m eq $masters[0];
        }
    }
    foreach (
        @{
            $lib->selectall_arrayref(
                    'select RKKeyword.name, RKVersion.uuid'
                  . ' from RKKeywordForVersion, RKVersion, RKKeyword'
                  . ' where RKVersion.modelId = RKKeywordForVersion.versionId'
                  . ' and RKKeyword.modelId = RKKeywordForVersion.keywordId'
                  . ' order by RKKeyword.name'
            )
        }
      )
    {
        my ( $keyword, $version ) = @$_;
        push @{ $metadata{versionsByKeyword}{$keyword} }, $version;
    }
    foreach (
        @{
            $lib->selectall_arrayref(
                'select uuid, fileIsReference, imagePath from RKMaster'
            )
        }
      )
    {
        my ( $muuid, $externalFlag, $path ) = @$_;
        if ($externalFlag) {
            $metadata{fileByMaster}{$muuid} = "/$path";
        }
        else {
            $metadata{fileByMaster}{$muuid} = $path;
            my ( $a, $b, $c, $d, $e ) = split m#/#, $path;
            $metadata{starsByFile}{$a}{$b}{$c}{$d}{$e} =
              $metadata{starsById}{$muuid};
        }
    }
    \%metadata;
}

sub scan {
    my ( $lib, $hints ) = @_;
    my $stat = $hints->statFromGid( $lib->repairPermissions );
    $hints->beginInteractive;
    warn "Scanning $lib->[LIB_DIR]/Masters";
    FileMgt106::Scanner->new( "$lib->[LIB_DIR]/Masters", $hints, $stat )
      ->scan( time - 27 )
      if -d "$lib->[LIB_DIR]/Masters";
    warn "Scanning $lib->[LIB_DIR]";
    my $scalar =
      FileMgt106::Scanner->new( $lib->[LIB_DIR], $hints, $stat )->scan(0);
    $hints->commit;
    $scalar->{'/FilterFactory::Aperture'} = $lib->extractApertureMetadata;
    $scalar;
}

sub findOrMakeApertureLibraries {
    my $class   = shift;
    my $hints   = shift;
    my @liblist = map { /\.aplibrary(\/*|\.jbz)$/s ? decode_utf8($_) : (); } @_;
    foreach my $lib ( grep { s/\.jbz$//; } @liblist ) {
        my @stat = stat "$lib.jbz";
        unless (@stat) {
            warn "Ignored: $lib.jbz";
            next;
        }
        eval {
            unless ( -d $lib ) {
                mkdir $lib or die "Failed to mkdir $lib";
            }
            require FileMgt106::LoadSave;
            my $target = FileMgt106::LoadSave::loadNormalisedScalar("$lib.jbz");
            delete $target->{$_} foreach grep { /\//; } keys %$target;
            $hints->beginInteractive;
            FileMgt106::Scanner->new( $lib, $hints,
                $hints->statFromGid( $stat[STAT_GID] ) )->scan( 0, $target );
            $hints->commit;
            utime time, $stat[STAT_MTIME], $lib;
        };
        warn "Failed to rebuild $lib: $@" if $@;
    }
    unless (@liblist) {
        warn 'Looking for Aperture libraries';
        my $pathFinder=$hints->{pathFinderFactory}->();
        $hints->beginInteractive;
        @liblist =
          map { defined $_ ? decode_utf8($_) : (); }
          map { $pathFinder->( $_->[0] ); } @{
            $hints->{dbHandle}->selectall_arrayref(
                'select locid from locations where name like "%.aplibrary"')
          };
        $hints->commit;
    }
    map {
        my @t = stat "$_/Database/apdb";
        @t ? $class->new( $_, $t[STAT_MTIME] ) : ();
    } @liblist;
}

sub setPathsCheckUpToDate {
    my ( $lib, $jbzDir ) = @_;
    chdir $lib->[LIB_DIR] or return;
    local $_ = $lib->[LIB_DIR] = decode_utf8 getcwd();
    {
        my $location;
        if (s#/Volumes/(.*)/##gs) {
            $location = $1;
        }
        elsif (s#(.*)/##gs) {
            $location = $1;
            local $_ = hostname();
            s/\..*//;
            $location = $_ . $location;
        }
        if ($location) {
            $location =~ tr#/#.#;
            s/\.aplibrary$/ in $location/s;
        }
    }
    if ( length( encode_utf8 $_) > 200 ) {
        require Digest::SHA;
        $_ = substr( $_, 0, 150 ) . ' ' . Digest::SHA::sha1_hex($_);
    }
    my @stat = stat( $lib->[LIB_JBZ] = "$jbzDir/$_.aplibrary.jbz" );
    @stat and $stat[STAT_MTIME] > $lib->[LIB_MTIME];
}

sub updateJbz {
    my ( $lib, $hints, $jbzDir ) = @_;
    return if $lib->setPathsCheckUpToDate($jbzDir);
    my $scalar = $lib->scan($hints);
    require FileMgt106::LoadSave;
    FileMgt106::LoadSave::saveJbz( $lib->[LIB_JBZ] . $$, $scalar );
    rename $lib->[LIB_JBZ] . $$, $lib->[LIB_JBZ];
}

1;
