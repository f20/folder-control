package Daemon112::KQueue;

# Copyright 2008-2016 Franck Latrémolière, Reckon LLP.
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

# BSD-specific variables and methods

use IO::KQueue;
use Fcntl;
use constant O_EVTONLY => 0x8000;
use base 'Exporter';
our @EXPORT_OK = qw(O_EVTONLY
  KQ_IDENT KQ_FILTER KQ_FLAGS KQ_FFLAGS KQ_DATA KQ_UDATA
  EVFILT_VNODE EVFILT_READ
  EV_ADD EV_CLEAR EV_DELETE
  NOTE_WRITE NOTE_DELETE NOTE_RENAME NOTE_ATTRIB);
our %EXPORT_TAGS = ( constants => \@EXPORT_OK );

use Scalar::Util qw(weaken);
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
    bless {
        kq         => IO::KQueue->new,
        last_udata => 42,
      },
      $_[0];
}

sub EV_SET {
    my $me = $_[0];
    my $udata;
    $me->{ $udata = ++$me->{last_udata} } = $_[6] if defined $_[6];
    $me->{kq}->EV_SET( @_[ 1 .. 5 ], $udata );
}

sub kevent {
    my ( $me, $ms ) = @_;
    map {
        my @e = @$_;
        $e[KQ_UDATA] = $me->{ $e[KQ_UDATA] }
          if defined $e[KQ_UDATA] && $me->{ $e[KQ_UDATA] };
        \@e;
    } $me->{kq}->kevent($ms);
}

sub delete_objects {
    my ( $me, $filter ) = @_;
    foreach ( grep { /^[0-9]+$/s } keys %$me ) {
        delete $me->{$_} if $filter->( $me->{$_} );
    }
}

# BSD-specific implementation of generic methods

sub events {
    my ( $me, $seconds ) = @_;
    map { [ defined $_->[KQ_UDATA] ? $me->{ $_->[KQ_UDATA] } : undef, $_ ]; }
      $me->{kq}->kevent( 1000 * $seconds );
}

sub startWatching {
    my ( $me, $obj, $path, $priority ) = @_;
    if ( keys %watchedByFileno > $thresholdWatchCount ) {
        warn "thresholdWatchCount ($thresholdWatchCount) reached";
        my @todelete = (
            sort {
                ( $priorityByFileno{$a} || 0 )
                  <=> ( $priorityByFileno{$b} || 0 );
            } keys %watchedByFileno
        )[ 0 .. 15 ];
        warn "Stop watching filenos @todelete";
        $me->stopWatching( $watchedByFileno{$_}, $_ ) foreach @todelete;
    }
    my $sh;
    sysopen $sh, $path, O_EVTONLY
      or do {
        warn "sysopen '$path': $! in " . `pwd`;
        return;
      };
    my $no = fileno $sh;
    $me->EV_SET(
        $no, EVFILT_VNODE,
        EV_ADD | EV_CLEAR,
        NOTE_WRITE | NOTE_DELETE | NOTE_RENAME,
        0, $obj
    );
    $obj->{timestamp} = time;
    $obj->{sysHandle}{$sh} = $sh;
    weaken( $watchedByFileno{$no} = $obj );
    $priorityByFileno{$no} = ( $priority || 0 ) + rand();
}

sub stopWatching {
    my ( $me, $obj, $fileno ) = @_;
    my %remainingSysHandles;
    foreach ( grep { $_ } values %{ $obj->{sysHandle} } ) {
        my $no = fileno $_;
        if ( !defined $fileno || $no == $fileno ) {
            $me->EV_SET( $no, EVFILT_VNODE, EV_DELETE,
                NOTE_WRITE | NOTE_DELETE | NOTE_RENAME,
                0, $obj );
            close $_;
            delete $watchedByFileno{$no};
        }
        else {
            $remainingSysHandles{$_} = $_;
        }
    }
    $me->delete_objects( sub { !( $_[0] - $obj ); } )
      unless %remainingSysHandles;
    $obj->{sysHandle} = \%remainingSysHandles;
}

sub stopWatchingOnDeath {
    my ( $me, $obj, $kevent ) = @_;
    $me->stopWatching( $obj, $kevent->[KQ_IDENT] )
      if ( ( NOTE_DELETE | NOTE_RENAME ) & $kevent->[KQ_FFLAGS] );
}

1;
