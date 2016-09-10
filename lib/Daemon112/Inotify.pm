package Daemon112::Inotify;

# Basic partial implementation of the same public interface as Daemon112::KQueue.

=head Copyright licence and disclaimer

Copyright 2016 Franck Latrémolière, Reckon LLP.

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

use warnings;
use strict;
use Linux::Inotify2;

sub new {
    bless {
        queue      => Linux::Inotify2->new,
        last_udata => 1,
      },
      $_[0];
}

sub events {
    my ( $me, $seconds ) = @_;
    return unless $seconds;
    my @events;
    eval {
        local $SIG{__DIE__} = undef;
        local $SIG{ALRM} = sub { die "alarm\n" };    # NB: \n required
        alarm $seconds;
        @events = $me->{queue}->read;
        alarm 0;
    };
    if ($@) {
        die unless $@ eq "alarm\n";    # propagate unexpected errors
        return;
    }
    map { [ $me->{ 0 + $_->w }, $_ ]; } @events;
}

sub startWatching {
    my ( $me, $obj, $path ) = @_;
    my $watcher = $me->{queue}->watch( $path,
        IN_MODIFY | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE |
          IN_DELETE_SELF | IN_MOVE_SELF );
    $me->{ 0 + $watcher } = $obj;
    $me->{ 0 + $obj }     = $watcher;
}

sub stopWatching {
    my ( $me, $obj ) = @_;
    $me->{ 0 + $obj }->cancel;
}

sub stopWatchingIfDead { }

1;
