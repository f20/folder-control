package FileMgt106::Extraction::MetadataReports;

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
use FileMgt106::Extraction::Metadatabase;

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
      CreateDate
      ModifyDate
      PageCount
      DateCreated
      DateTimeCreated
      ImageCount
    );
}

sub metadataProcessorMaker {
    my ($mdbFile) = @_;
    my @tags = tagsToUse();
    sub {
        my ($fileWriter) = @_;
        $fileWriter->( qw(sha1 mtime size ext name folder), @tags );
        my ( $extractionWorkerPre, $extractionWorkerDo ) =
          FileMgt106::Extraction::Metadatabase::metadataExtractionWorker();
        my ( $storageWorkerPre, $storageWorkerDo ) =
          FileMgt106::Extraction::Metadatabase::metadataStorageWorker( $mdbFile,
            $fileWriter, \@tags );
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
          FileMgt106::Extraction::Metadatabase::metadataStorageWorker( $mdbFile,
            $fileWriter, \@tags );
        my ( $extractionWorkerPre, $extractionWorkerDo ) =
          FileMgt106::Extraction::Metadatabase::metadataExtractionWorker();

        require Thread::Pool;
        require Thread::Queue;
        my $queue = Thread::Queue->new;

        my $storageThread = threads->create(
            sub {
                my $workerPool = Thread::Pool->new(
                    {
                        pre => $extractionWorkerPre,
                        do  => sub {
                            my %hash = @_;
                            if ( my $p = delete $hash{extractionPath} ) {
                                $queue->insert( 5,
                                    $extractionWorkerDo->( $p, \%hash ) );
                            }
                        },
                        workers => 12,
                    }
                );
                $storageWorkerPre->();
                while (1) {
                    my $arg = $queue->dequeue;
                    unless ( ref $arg ) {
                        if ( $queue->pending ) {
                            $queue->enqueue($arg);
                            next;
                        }
                        if ( $arg == 1 ) {
                            $workerPool->shutdown;
                            $queue->enqueue(2);
                            next;
                        }
                        $storageWorkerDo->();
                        last;
                    }
                    if ( my $wantMore = $storageWorkerDo->($arg) ) {
                        sleep 1 while $workerPool->todo > 96;
                        $workerPool->job( map { "$_"; } %$wantMore );
                    }
                }
            }
        );

        $queue->enqueue(
            {
                map { ( $_ => $_ ); } qw(sha1 mtime size ext name folder path),
                @tags
            }
        );
        sub {
            if ( my ( $sha1, $mtime, $size, $ext, $name, $folder ) = @_ ) {
                $queue->enqueue(
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
            }
            else {
                $queue->enqueue(1);
                $storageThread->join;
            }
        };
    };

}

1;
