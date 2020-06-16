package Daemon112::Watcher;

# Copyright 2011-2018 Franck Latrémolière, Reckon LLP and others.
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

use warnings;
use strict;
use overload
  '""' => sub { $_[0]{name} || 'Anonymous watcher ' . ( 0 + $_[0] ); },
  '0+' => sub { $_[0] },
  fallback => 1;

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
    $kqueue->startWatching( $self, $path, $priority );
    $self;
}

sub stopWatching {
    my ( $self, $kqueue, $fileno ) = @_;
    $kqueue->stopWatching( $self, $fileno );
    $self;
}

sub kevented {
    my ( $self, $runner, $kevent ) = @_;
    $runner->{kq}->stopWatchingOnDeath( $self, $kevent );
    $self->schedule( time + $self->{delay}, $runner->{pq} );
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
