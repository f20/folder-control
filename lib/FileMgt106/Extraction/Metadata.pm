package FileMgt106::Extraction::Metadata;

# Copyright 2017-2021 Franck Latrémolière and others.
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
use File::Spec::Functions qw(catfile);
use DBD::SQLite;
use Digest::SHA;
use Digest::SHA3;
use Image::ExifTool;

sub tagsToUse {
    qw(
      DateTimeOriginal
      ImageHeight
      ImageWidth
      LensID
      LensSpec
      MemoryCardNumber
      SerialNumber
      ShutterCount
      );    # These might also be useful for some cameras:
            # CreateDate DateCreated DateTimeCreated ImageCount
}

sub metadataExtractionWorker {
    my ( $sha1, $sha512224, $sha3, $shake128, $objExifTool );
    my $preWorker = sub
    {    # Digest::SHA and Digest::SHA3 are thread-safe in FreeBSD 11 perl 5.2x
            # but not in macOS perl 5.18
        $sha1        = Digest::SHA->new;
        $sha512224   = Digest::SHA->new(512224);
        $sha3        = Digest::SHA3->new;
        $shake128    = Digest::SHA3->new(128000);
        $objExifTool = Image::ExifTool->new;
    };
    my $worker = sub {
        my ( $path, $basics ) = @_;
        warn "$path\n";
        my $results =
          $path =~
          /\.(?:jpg|jpeg|tif|tiff|psd|png|nef|arw|raw|m4a|mp3|mp4|heic|heif)$/is
          ? $objExifTool->ImageInfo($path)
          : {};
        $results->{'SHA-1'}       = $sha1->addfile($path)->hexdigest;
        $results->{'SHA-512/224'} = $sha512224->addfile($path)->b64digest;
        $results->{'SHA-3/224'}   = $sha3->addfile($path)->b64digest;
        $results->{bytes}         = -s $path;
        $results->{path}          = $path;

        while ( my ( $k, $v ) = each %$basics ) {
            $results->{$k} = $v;
        }
        $results;
    };
    $preWorker, $worker;
}

sub metadataStorageWorker {
    my ( $mdbFile, $fileWriter, $tags ) = @_;
    my ( $mdbh, $getid, $qGetSub, $qGetProps, $qAddSub, $qAddRel );
    my $counter = -1;
    my %seen;

    my $storagePreWorker = sub {
        $mdbh = DBI->connect( "dbi:SQLite:dbname=$mdbFile",
            { sqlite_unicode => 0, AutoCommit => 0, } );
        do { sleep 1 while !$mdbh->do($_); }
          foreach grep { $_ } split /;\s*/s, <<EOSQL;
pragma temp_store = memory;
begin immediate transaction;
create table if not exists subj (s integer primary key, sha1 text);
create unique index if not exists subjsha1 on subj (sha1);
create table if not exists dic (p integer primary key, description text);
create unique index if not exists dicdes on dic (description);
create table if not exists rel (s integer, p integer, d text);
create unique index if not exists relsp on rel (s, p);
EOSQL
        my $qGetId = $mdbh->prepare('select p from dic where description=?');
        my $qAddDic =
          $mdbh->prepare('insert into dic (description) values (?)');
        $qGetSub = $mdbh->prepare('select s from subj where sha1=?');
        $qGetProps =
          $mdbh->prepare(
            'select description, d from dic inner join rel using (p) where s=?'
          );
        $qAddSub = $mdbh->prepare('insert into subj (sha1) values (?)');
        $qAddRel =
          $mdbh->prepare(
            'insert or replace into rel (s, p, d) values (?, ?, ?)');
        my %ids;
        $getid = sub {
            my ($description) = @_;
            return $ids{$description} if exists $ids{$description};
            $qGetId->execute($description);
            my ($id) = $qGetId->fetchrow_array;
            $qGetId->finish;
            return $ids{$description} = $id if $id;
            $qAddDic->execute($description);
            $qGetId->execute($description);
            ($id) = $qGetId->fetchrow_array;
            $qGetId->finish;
            return $ids{$description} = $id if $id;
        };
    };

    my $storageWorker = sub {

        my ($info) = @_;
        unless ($info) {
            $mdbh->commit;
            $mdbh->disconnect;
            $fileWriter->();
            return;
        }

        if ( undef && !defined $info->{path} && exists $seen{ $info->{sha1} } )
        {
            $fileWriter->(
                $info->{sha1}, [ $info->{mtime} ],
                [ $info->{size} ], $info->{ext},
                $info->{name},     $info->{folder},
            );
            return;
        }
        undef $seen{ $info->{sha1} };

        $qGetSub->execute( $info->{sha1} );
        my ($s) = $qGetSub->fetchrow_array;
        $qGetSub->finish;
        my %combo;
        if ($s) {
            $qGetProps->execute($s);
            while ( my ( $p, $v ) = $qGetProps->fetchrow_array ) {
                $combo{$p} = $v;
            }
        }
        while ( my ( $p, $v ) = each %$info ) {
            $combo{$p} = $v;
        }
        return $info unless $s || defined $info->{path};

        $fileWriter->(
            $info->{sha1},
            [ $info->{mtime} ],
            [ $info->{size} ],
            $info->{ext},
            $info->{name},
            $info->{folder},
            map { defined $_ ? [$_] : ['']; } @combo{@$tags}
        );

        return if $s;

        if ( --$counter < 0 ) {
            $mdbh->commit;
            sleep 1 while !$mdbh->do('begin immediate transaction');
            $counter = 42;
        }
        $qAddSub->execute( $info->{sha1} );
        $qGetSub->execute( $info->{sha1} );
        ($s) = $qGetSub->fetchrow_array;
        $qGetSub->finish;
        delete $info->{name};
        delete $info->{path};
        delete $info->{FileName};

        while ( my ( $k, $v ) = each %$info ) {
            next unless defined $v;
            $qAddRel->execute( $s, $getid->($k),
                'SCALAR' eq ref $v ? $$v : $v );
        }

    };

    $storagePreWorker, $storageWorker;

}

sub metadataProcessorMaker {
    my ($mdbFile) = @_;
    my @tags = tagsToUse();
    sub {
        my ($fileWriter) = @_;
        $fileWriter->( qw(sha1 mtime size ext name folder), @tags );
        my ( $extractionWorkerPre, $extractionWorkerDo ) =
          metadataExtractionWorker();
        my ( $storageWorkerPre, $storageWorkerDo ) =
          metadataStorageWorker( $mdbFile, $fileWriter, \@tags );
        $extractionWorkerPre->();
        $storageWorkerPre->();
        sub {
            return $storageWorkerDo->() unless @_;
            my ( $sha1, $mtime, $size, $ext, $name, $folder ) = @_;
            my $info = $storageWorkerDo->(
                {
                    sha1   => $sha1,
                    mtime  => $mtime,
                    size   => $size,
                    ext    => $ext,
                    name   => $name,
                    folder => $folder,
                }
            );
            $storageWorkerDo->(
                $extractionWorkerDo->( catfile( $folder, $name ), $info ) )
              if $info;
        };
    };
}

sub metadataThreadedProcessorMaker {
    my ($mdbFile) = @_;
    my @tags = tagsToUse();
    sub {
        my ($fileWriter) = @_;
        my ( $storageWorkerPre, $storageWorkerDo ) =
          metadataStorageWorker( $mdbFile, $fileWriter, \@tags );
        my ( $extractionWorkerPre, $extractionWorkerDo ) =
          metadataExtractionWorker();
        require FileMgt106::Extraction::MetadataThreading;
        my $enqueuer = FileMgt106::Extraction::MetadataThreading::runPoolQueue(
            $extractionWorkerPre, $extractionWorkerDo, $storageWorkerPre,
            $storageWorkerDo,     $fileWriter,
        );
        $enqueuer->(
            {
                map { ( $_ => $_ ); } qw(sha1 mtime size ext name folder path),
                @tags
            }
        );
        sub {
            return $enqueuer->() unless @_;
            my ( $sha1, $mtime, $size, $ext, $name, $folder ) = @_;
            $enqueuer->(
                {
                    extractionPath => catfile( $folder, $name ),
                    sha1           => $sha1,
                    mtime          => $mtime,
                    size           => $size,
                    ext            => $ext,
                    name           => $name,
                    folder         => $folder,
                }
            );
        };
    };
}

1;
