package FileMgt106::Threading;

=head Copyright licence and disclaimer

Copyright 2017-2018 Franck Latrémolière, Reckon LLP.

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

use threads;
use Thread::Pool;
use Thread::Queue;

sub runPoolQueue {

    my (
        $extractionWorkerPre, $extractionWorkerDo, $storageWorkerPre,
        $storageWorkerDo,     $fileWriter,
    ) = @_;

    my $queue = Thread::Queue->new;

    my $storageThread = threads->create(
        sub {
            my $workerPool = Thread::Pool->new(
                {
                    pre => $extractionWorkerPre,
                    do  => sub {
                        my %hash = @_;
                        if ( my $p = delete $hash{extractionPath} ) {
                            $queue->enqueue(
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
                    if ( $arg == 1 ) {
                        $workerPool->shutdown;
                        $queue->enqueue(2);
                        next;
                    }
                    $storageWorkerDo->();
                    last;
                }
                if ( my $wantMore = $storageWorkerDo->($arg) ) {

                    # sleep 1 while $workerPool->todo > 64;
                    $workerPool->job( map { "$_"; } %$wantMore );
                }
            }
        }
    );

    sub {
        my ($arg) = @_;
        unless ( defined $arg ) {
            $queue->enqueue(1);
            $storageThread->join;
            return;
        }
        sleep 1 while $queue->pending > 255;
        $queue->enqueue($arg);
    };

}

1;
