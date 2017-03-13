package FileMgt106::ScannerAperture;

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
use FileMgt106::Scanner;
use FileMgt106::FileSystem;

sub repairPermissions {
    my ( $self, $libdir ) = @_;
    unless ( chdir $libdir ) {
        warn die "Cannot chdir $libdir";
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
                        warn "Cannot rename to $_ in $libdir";
                        next;
                    }
                    @stat = lstat $_;
                }
                if ( $stat[STAT_NLINK] > 1 ) {
                    warn "$_ still multilinked in $libdir";
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
        local $_ = $libdir;
        s/\.aplibrary$/ (Masters)/;
        unlink $_;
        symlink "$libdir/Masters", $_;
    }
    $rgid;
}

sub extractApertureMetadata {
    my ( $self, $libdir ) = @_;
    return unless -s "$libdir/Database/apdb/Library.apdb";
    my $lib =
      DBI->connect( "dbi:SQLite:dbname=$libdir/Database/apdb/Library.apdb",
        '', '', { sqlite_unicode => 0, AutoCommit => 1, } );
    unless ($lib) {
        warn "Cannot open $libdir/Database/apdb/Library.apdb";
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
    my ( $self, $libdir, $hints ) = @_;
    my $stat = $hints->statFromGid( $self->repairPermissions($libdir) );
    $hints->beginInteractive;
    warn "Scanning $libdir/Masters";
    FileMgt106::Scanner->new( "$libdir/Masters", $hints, $stat )
      ->scan( time - 27 )
      if -d "$libdir/Masters";
    warn "Scanning $libdir";
    my $scalar = FileMgt106::Scanner->new( $libdir, $hints, $stat )->scan(0);
    $hints->commit;
    $scalar->{'/FilterFactory::Aperture'} =
      $self->extractApertureMetadata($libdir);
    $scalar;
}

1;
