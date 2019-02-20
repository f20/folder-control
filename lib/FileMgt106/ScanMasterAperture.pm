package FileMgt106::ScanMasterAperture;

# Copyright 2011-2019 Franck Latrémolière.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use strict;
use warnings;
use Cwd;
use Encode qw(decode_utf8 encode_utf8);
use FileMgt106::FileSystem;
use FileMgt106::ScanMaster;
use FileMgt106::Scanner;

our @ISA = 'FileMgt106::ScanMaster';

use constant {
    SM_DIR       => 0,
    SM_REPOPAIR  => 4,
    SM_ROOTLOCID => 6,
    SM_SCALAR    => 7,
    SM_WATCHING  => 14,
};

sub repairPermissions {
    my ($self) = @_;
    unless ( chdir $self->[SM_DIR] ) {
        warn die "Cannot chdir $self->[SM_DIR]";
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
                        warn "Cannot rename to $_ in $self->[SM_DIR]";
                        next;
                    }
                    @stat = lstat $_;
                }
                if ( $stat[STAT_NLINK] > 1 ) {
                    warn "$_ still multilinked in $self->[SM_DIR]";
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
        local $_ = $self->[SM_DIR];
        s/\.aplibrary$/ (Masters)/;
        unlink $_;
        symlink "$self->[SM_DIR]/Masters", $_;
    }
    $rgid;
}

sub extractApertureMetadata {
    my ($self) = @_;
    return unless -s "$self->[SM_DIR]/Database/apdb/Library.apdb";
    my $libDbh =
      DBI->connect(
        "dbi:SQLite:dbname=$self->[SM_DIR]/Database/apdb/Library.apdb",
        '', '', { sqlite_unicode => 0, AutoCommit => 1, } );
    unless ($libDbh) {
        warn "Cannot open $self->[SM_DIR]/Database/apdb/Library.apdb";
        return;
    }
    my %metadata;
    foreach (
        @{
            $libDbh->selectall_arrayref(
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
            $libDbh->selectall_arrayref(
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
            $libDbh->selectall_arrayref(
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
    my ( $self, $hints, $rgid, $frotl ) = @_;
    my $stat = $hints->statFromGid( $self->repairPermissions );
    FileMgt106::Scanner->new( "$self->[SM_DIR]/Masters", $hints, $stat )
      ->scan( time - 7, undef, undef, $self->[SM_REPOPAIR] )
      if -d "$self->[SM_DIR]/Masters";
    @{$self}[ SM_SCALAR, SM_ROOTLOCID ] =
      FileMgt106::Scanner->new( $self->[SM_DIR], $hints, $stat )
      ->scan( undef, undef, undef, $self->[SM_REPOPAIR] );
    $self->[SM_SCALAR]{'/FilterFactory::Aperture'} =
      $self->extractApertureMetadata;
    $self->[SM_SCALAR]{Database} =
      FileMgt106::Scanner->new( "$self->[SM_DIR]/Database", $hints, $stat )
      ->scan( undef, undef, undef, $self->[SM_REPOPAIR],
        $self->[SM_WATCHING] ? $self : undef )
      if -d "$self->[SM_DIR]/Database";
}

sub fullRescanTimeOffset {
    1_200;
}

1;
