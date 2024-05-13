package FileMgt106::Extraction::MetadataReports;

# Copyright 2017-2024 Franck Latrémolière and others.
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
use threads;

sub makeFiledataExtractor {
    ( undef, my $hintsFile ) = @_;
    binmode STDOUT, ':utf8';
    print join( "\t", qw(catalogue sha1 modified bytes filename) ) . "\n";
    require POSIX;
    require FileMgt106::Database;
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    my $query =
      $hints->{dbHandle}
      ->prepare('select mtime, size from locations where sha1=?');
    sub {
        my ( $scalar, $catname ) = @_;
        $catname = '' unless defined $catname;
        my ( %seen, $processor );
        undef $seen{'da39a3ee5e6b4b0d3255bfef95601890afd80709'};
        $processor = sub {
            my ($cat) = @_;
            while ( my ( $k, $v ) = each %$cat ) {
                next unless defined $v;
                if ( 'HASH' eq ref $v ) {
                    $processor->($v);
                    next;
                }
                if ( $v =~ /([a-fA-F0-9]{40})/ ) {
                    my $sha1hex = lc $1;
                    next if exists $seen{$sha1hex};
                    $query->execute( pack( 'H*', $sha1hex ) );
                    my ( $time, $bytes ) = $query->fetchrow_array;
                    $query->finish;
                    undef $seen{$sha1hex};
                    print join( "\t",
                        $catname,
                        $v,
                        defined $time
                        ? POSIX::strftime( "%F %T", gmtime($time) )
                        : "\t",
                        defined $bytes ? $bytes : '',
                        $k,
                    ) . "\n";
                }
            }
        };
        $processor->($scalar);
        return;
    };
}

sub makeMetadataExtractor {

    my ( $self_discard, $hintsFile, $mdbFile, $tsvStream, $nWorkers, $shape, )
      = @_;
    $tsvStream ||= \*STDOUT;
    binmode $tsvStream, ':utf8';
    $nWorkers ||= 12;
    my @properties = $shape eq 'tall'
      ? qw(
      CreateDate
      DateCreated
      DateTimeCreated
      DateTimeOriginal
      GPSPosition
      ImageCount
      ImageHeight
      ImageWidth
      LensID
      LensSpec
      MemoryCardNumber
      ModifyDate
      PageCount
      PDFVersion
      SerialNumber
      ShutterCount
      )
      : $shape eq 'wide' ? qw(
      DateTimeOriginal
      GPSPosition
      ImageHeight
      ImageWidth
      SerialNumber
      ShutterCount
      )
      : qw();

    my $outputWriter = @properties > 7
      ? sub {
        my ( $part1, $part2 ) = @_ or return;
        my $sha1hex = $part1->{sha1hex};
        print {$tsvStream} join( "\t", $sha1hex, $_, $part1->{$_} ) . "\n"
          foreach qw(filename);
        print {$tsvStream} join( "\t", $sha1hex, $_, $part2->{$_} ) . "\n"
          foreach grep { defined $part2->{$_}; } @properties;
      }
      : sub {
        my ( $part1, $part2 ) = @_ or return;
        print {$tsvStream} join( "\t",
            $part1->{sha1hex}, $part1->{filename},
            map { defined $_ ? $_ : ''; } @{$part2}{@properties} )
          . "\n";
      };

    require FileMgt106::Database;

    require Thread::Queue;
    my $queue = Thread::Queue->new;

    require FileMgt106::Extraction::Metadatabase;

    my $storageThread = threads->create(
        sub {
            require Thread::Pool;
            my ( $extractionWorkerPre, $extractionWorkerDo ) =
              FileMgt106::Extraction::Metadatabase::metadataExtractionWorker();
            my $pathFromSha1hex;
            my $extractionPool = Thread::Pool->new(
                {
                    pre => sub {
                        $extractionWorkerPre->();
                        my $hints = FileMgt106::Database->new( $hintsFile, 1 );
                        my $searchSha1 = $hints->{searchSha1};
                        $pathFromSha1hex = sub {
                            my ($sha1hex) = @_;
                            my $iterator =
                              $searchSha1->( pack( 'H*', $sha1hex ) );
                            while ( my ($path) = $iterator->() ) {
                                return $path if -f $path;
                            }
                            return;
                        };
                    },
                    do => sub {
                        my (%extractionSource) = @_;
                        $queue->enqueue(
                            {
                                _extractSource_ => \%extractionSource,
                                _extractResult_ => $extractionWorkerDo->(
                                    $pathFromSha1hex->(
                                        $extractionSource{sha1hex}
                                    )
                                ),
                            }
                        );
                    },
                    workers => $nWorkers,
                }
            );
            my ( $storageSetup, $storageReader, $storageWriter ) =
              FileMgt106::Extraction::Metadatabase::metadataStorageWorkers(
                $mdbFile);
            $storageSetup->();
            while (1) {
                my $item = $queue->dequeue;
                if ( my $finishUpStage = $item->{_finishUp_} ) {
                    if ( $finishUpStage == 1 ) {
                        $extractionPool->shutdown;
                        $queue->enqueue( { _finishUp_ => 1 + $finishUpStage } );
                        next;
                    }
                    if ( $queue->pending ) {
                        $queue->enqueue( { _finishUp_ => 1 + $finishUpStage } );
                        next;
                    }
                    else {
                        $storageWriter->();
                        $outputWriter->();
                        last;
                    }
                }
                if ( my $extractionSource = $item->{_extractSource_} ) {
                    $storageWriter->(
                        $extractionSource->{sha1hex},
                        $item->{_extractResult_}
                    );
                    $outputWriter->(
                        $extractionSource, $item->{_extractResult_}
                    );
                    next;
                }
                if ( my $sha1hex = $item->{sha1hex} ) {
                    my $info = $storageReader->($sha1hex);
                    if (  !%$info
                        || $item->{filename} =~ /\.pdf$/si
                        && !$info->{PDFVersion} )
                    {
                        sleep 1 while $extractionPool->todo > 96;
                        $extractionPool->job( map { "$_"; } %$item );
                        next;
                    }
                    $outputWriter->( $item, $info );
                    next;
                }
                die $item;
            }
        }
    );

    sub {
        my ( $scalar, $catname ) = @_;
        unless ( defined $scalar ) {
            $queue->enqueue( { _finishUp_ => 1 } );
            $storageThread->join;
            return;
        }
        $catname = '' unless defined $catname;
        my ( %seen, $processor );
        $processor = sub {
            my ($cat) = @_;
            while ( my ( $k, $v ) = each %$cat ) {
                if ( 'HASH' eq ref $v ) {
                    $processor->($v);
                    next;
                }
                if ( $v =~ /([a-fA-F0-9]{40})/ ) {
                    my $sha1hex = lc $1;
                    next if exists $seen{$sha1hex};
                    undef $seen{$sha1hex};
                    $queue->enqueue(
                        {
                            catname  => $catname,
                            filename => $k,
                            sha1hex  => $sha1hex,
                        }
                    );
                }
            }
        };
        $processor->($scalar);
        return;
    };

}

sub makeMetadataWideProcessor {

    ( undef, my $mdbFile, my $nWorkers ) = @_;
    $nWorkers ||= 12;
    my @tags = qw(
      DateTimeOriginal
      ImageHeight
      ImageWidth
      LensID
      LensSpec
      MemoryCardNumber
      SerialNumber
      ShutterCount
      PDFVersion
    );
    require FileMgt106::Extraction::Metadatabase;

    return sub {
        my ($fileWriter) = @_;
        my ( $extractionWorkerPre, $extractionWorkerDo ) =
          FileMgt106::Extraction::Metadatabase::metadataExtractionWorker();
        my ( $storageSetup, $storageReader, $storageWriter ) =
          FileMgt106::Extraction::Metadatabase::metadataStorageWorkers(
            $mdbFile);
        $extractionWorkerPre->();
        $storageSetup->();
        $fileWriter->( qw(sha1 mtime size ext name folder), @tags );
        sub {
            unless (@_) {
                $storageWriter->();
                $fileWriter->();
                return;
            }
            my ( $sha1, $mtime, $size, $ext, $name, $folder ) = @_;
            my $info = $storageReader->($sha1);
            unless (%$info) {
                $info = $extractionWorkerDo->( catfile( $folder, $name ) );
                $storageWriter->( $sha1, $info );
            }
            $fileWriter->(
                $sha1, [$mtime], [$size], $ext, $name, $folder,
                map { defined $_ ? [$_] : ['']; } @{$info}{@tags}
            );
        };
      }
      if $nWorkers == 1;

    sub {

        my ($fileWriter) = @_;

        require Thread::Queue;
        my $queue = Thread::Queue->new;

        my $storageThread = threads->create(
            sub {
                require Thread::Pool;
                my ( $extractionWorkerPre, $extractionWorkerDo ) =
                  FileMgt106::Extraction::Metadatabase::metadataExtractionWorker(
                  );
                my $extractionPool = Thread::Pool->new(
                    {
                        pre => $extractionWorkerPre,
                        do  => sub {
                            my ( $extractionPath, %identification ) = @_;
                            my $extracted =
                              $extractionWorkerDo->($extractionPath);
                            $queue->enqueue(
                                {
                                    id        => \%identification,
                                    extracted => $extracted
                                }
                            );
                        },
                        workers => $nWorkers,
                    }
                );
                my ( $storageSetup, $storageReader, $storageWriter ) =
                  FileMgt106::Extraction::Metadatabase::metadataStorageWorkers(
                    $mdbFile);
                $storageSetup->();
                while (1) {
                    my $item = $queue->dequeue;
                    if ( my $finishUpStage = $item->{finishUpStage} ) {
                        if ( $finishUpStage == 1 ) {
                            $extractionPool->shutdown;
                            $queue->enqueue(
                                { finishUpStage => 1 + $finishUpStage } );
                            next;
                        }
                        if ( $queue->pending ) {
                            $queue->enqueue(
                                { finishUpStage => 1 + $finishUpStage } );
                            next;
                        }
                        else {
                            $storageWriter->();
                            $fileWriter->();
                            last;
                        }
                    }
                    if ( my $id = $item->{id} ) {
                        $storageWriter->( $id->{sha1}, $item->{extracted} );
                        $queue->enqueue(
                            { w1 => $id, w2 => $item->{extracted} } );
                        next;
                    }
                    if ( my $sha1 = $item->{sha1} ) {
                        my $info = $storageReader->($sha1);
                        unless (%$info) {
                            sleep 1 while $extractionPool->todo > 96;
                            $extractionPool->job(
                                catfile( $item->{folder}, $item->{name} ),
                                map { "$_"; } %$item );
                            next;
                        }
                        $queue->enqueue( { w1 => $item, w2 => $info } );
                        next;
                    }
                    $fileWriter->(
                        $item->{w1}{sha1},
                        [ $item->{w1}{mtime} ],
                        [ $item->{w1}{size} ],
                        $item->{w1}{ext},
                        $item->{w1}{name},
                        $item->{w1}{folder},
                        map { defined $_ ? [$_] : ['']; }
                          @{ $item->{w2} }{@tags}
                    );
                }
            }
        );

        $fileWriter->( qw(sha1 mtime size ext name folder), @tags );

        sub {
            if ( my ( $sha1, $mtime, $size, $ext, $name, $folder ) = @_ ) {
                $queue->enqueue(
                    {
                        ext    => $ext,
                        folder => $folder,
                        mtime  => $mtime,
                        name   => $name,
                        sha1   => $sha1,
                        size   => $size,
                    }
                );
            }
            else {
                $queue->enqueue( { finishUpStage => 1 } );
                $storageThread->join;
            }
        };

    };

}

1;
