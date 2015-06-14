package Daemon112::Watcher;

=head Copyright licence and disclaimer

Copyright 2011-2015 Franck Latrémolière, Reckon LLP.

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
use utf8;
use overload
  '""' => sub { $_[0]{name} || 'Anonymous watcher ' . ( 0 + $_[0] ); },
  '0+' => sub { $_[0] },
  fallback => 1;

use Scalar::Util qw(weaken);
use Daemon112::KQueue qw(:constants);
use BSD::Resource;

my $thresholdWatchCount = getrlimit(RLIMIT_NOFILE);

# RLIM_INFINITY does not work on Mac OS X 10.5
if ( $thresholdWatchCount < 2345 ) {
    setrlimit( RLIMIT_NOFILE, 2345, 2345 );
    $thresholdWatchCount = getrlimit(RLIMIT_NOFILE);
}
$thresholdWatchCount = int( 0.8 * $thresholdWatchCount );
our %watchedByFileno;
our %priorityByFileno;

sub new {
    my ( $className, $action, $name, $delay ) = @_;
    bless {
        action    => $action,
        name      => $name,
        delay     => defined $delay ? $delay : 7,
        sysHandle => {},
    }, $className;
}

sub startWatching {
    my ( $self, $kqueue, $path, $priority ) = @_;
    if ( keys %watchedByFileno > $thresholdWatchCount ) {
        warn "thresholdWatchCount ($thresholdWatchCount) reached";
        my @todelete = (
            sort {
                ( $priorityByFileno{$a} || 0 )
                  <=> ( $priorityByFileno{$b} || 0 );
            } keys %watchedByFileno
        )[ 0 .. 15 ];
        warn "Stop watching filenos @todelete";
        $watchedByFileno{$_}->stopWatching( $kqueue, $_ ) foreach @todelete;
    }
    my $sh;
    sysopen $sh, $path, O_EVTONLY
      or do {
        warn "sysopen '$path': $! in " . `pwd`;
        return;
      };
    my $no = fileno $sh;
    $kqueue->EV_SET(
        $no, EVFILT_VNODE,
        EV_ADD | EV_CLEAR,
        NOTE_WRITE | NOTE_DELETE | NOTE_RENAME,
        0, $self
    );
    $self->{timestamp} = time;
    $self->{sysHandle}{$sh} = $sh;
    weaken( $watchedByFileno{$no} = $self );
    $priorityByFileno{$no} = ( $priority || 0 ) + rand();
    $self;
}

sub stopWatching {
    my ( $self, $kqueue, $fileno ) = @_;
    my %remainingSysHandles;
    foreach ( grep { $_ } values %{ $self->{sysHandle} } ) {
        my $no = fileno $_;
        if ( !defined $fileno || $no == $fileno ) {
            $kqueue->EV_SET( $no, EVFILT_VNODE, EV_DELETE,
                NOTE_WRITE | NOTE_DELETE | NOTE_RENAME,
                0, $self );
            close $_;
            delete $watchedByFileno{$no};
        }
        else {
            $remainingSysHandles{$_} = $_;
        }
    }
    $kqueue->delete_objects( sub { !( $_[0] - $self ); } )
      unless %remainingSysHandles;
    $self->{sysHandle} = \%remainingSysHandles;
    $self;
}

sub kevented {
    my ( $self, $runner, $kevent ) = @_;
    $self->stopWatching( $runner->{kq}, $kevent->[KQ_IDENT] )
      if ( ( NOTE_DELETE | NOTE_RENAME ) & $kevent->[KQ_FFLAGS] );
    $self->schedule( ( $self->{timestamp} = time ) + $self->{delay},
        $runner->{pq} );
}

sub schedule {
    my ( $self, $ttr, $queue ) = @_;
    unless ( $self->{queue} && $self->{queue} == $queue ) {
        $self->{queue}->remove_item( $self->{qid}, sub { $_[0] == $self; } )
          if defined $self->{qid};
        $self->{queue} = $queue;
        delete $self->{qid};
    }
    if ( exists $self->{qid} ) {
        delete $self->{qid}
          unless $self->{ttr} <= $ttr || $self->{queue}->set_priority(
            $self->{qid},
            sub { $_[0] == $self },
            $self->{ttr} = $ttr
          );
    }
    $self->{qid} = $self->{queue}->enqueue( $self->{ttr} = $ttr, $self )
      unless exists $self->{qid};
    $self;
}

sub dequeued {
    my ( $self, $runner ) = @_;
    delete $self->{$_} foreach qw(qid queue);
    $self->{action}->($runner);
}

1;
