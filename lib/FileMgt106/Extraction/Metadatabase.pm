package FileMgt106::Extraction::Metadatabase;

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
use DBD::SQLite;
use Digest::SHA;
use Digest::SHA3;
use Image::ExifTool;

sub metadataExtractionWorker {
    my (
        $sha1Machine,     $sha512224Machine, $sha3Machine,
        $shake128Machine, $exiftoolMachine
    );
    my $setup = sub
    {    # Digest::SHA and Digest::SHA3 are thread-safe in FreeBSD 11 perl 5.2x
            # but not in macOS perl 5.18
        $sha1Machine      = Digest::SHA->new;
        $sha512224Machine = Digest::SHA->new(512224);
        $sha3Machine      = Digest::SHA3->new;
        $shake128Machine  = Digest::SHA3->new(128000);
        $exiftoolMachine  = Image::ExifTool->new;
    };
    my $worker = sub {
        my ($path) = @_;
        return {} unless defined $path;
        warn "$path\n";
        my $results =
          $path =~
/\.(?:arw|dng|heic|heif|jpeg|jpg|m4a|m4v|mp3|mov|mp4|nef|pdf|png|psd|raw|tif|tiff)$/is
          ? $exiftoolMachine->ImageInfo($path)
          : {};
        $results->{'SHA-1'} = $sha1Machine->addfile($path)->hexdigest;
        $results->{'SHA-512/224'} =
          $sha512224Machine->addfile($path)->b64digest;
        $results->{'SHA-3/224'} = $sha3Machine->addfile($path)->b64digest;
        $results->{bytes} = -s $path;
        $results;
    };
    $setup, $worker;
}

sub metadataStorageWorkers {

    my ($mdbFile) = @_;

    my ( $mdbh, $getid, $qGetSub, $qGetProps, $qAddSub, $qAddRel );
    my $counter = 42;

    my $storageSetup = sub {
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
        $qAddSub =
          $mdbh->prepare('insert or ignore into subj (sha1) values (?)');
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

    my $storageReader = sub {
        my ($sha1) = @_;
        $qGetSub->execute($sha1);
        my ($s) = $qGetSub->fetchrow_array;
        $qGetSub->finish;
        my %info;
        if ($s) {
            $qGetProps->execute($s);
            while ( my ( $p, $v ) = $qGetProps->fetchrow_array ) {
                $info{$p} = $v;
            }
        }
        \%info;
    };

    my $storageWriter = sub {
        unless (@_) {
            $mdbh->commit;
            $mdbh->disconnect;
            return;
        }
        my ( $sha1, $info ) = @_;
        if ( --$counter < 0 ) {
            $mdbh->commit;
            sleep 1 while !$mdbh->do('begin immediate transaction');
            $counter = 242;
        }
        $qAddSub->execute($sha1);
        $qGetSub->execute($sha1);
        my ($s) = $qGetSub->fetchrow_array;
        $qGetSub->finish;
        while ( my ( $k, $v ) = each %$info ) {
            next unless defined $v;
            $qAddRel->execute( $s, $getid->($k), $v );
        }
    };

    $storageSetup, $storageReader, $storageWriter;

}

1;
