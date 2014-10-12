package Daemon112::KQueue;

=head Copyright licence and disclaimer

Copyright 2008-2012 Franck Latrémolière, Reckon LLP.

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
use Fcntl;
use IO::KQueue;
use constant O_EVTONLY => 0x8000;
use base 'Exporter';
our @EXPORT_OK = qw(O_EVTONLY
  KQ_IDENT KQ_FILTER KQ_FLAGS KQ_FFLAGS KQ_DATA KQ_UDATA
  EVFILT_VNODE EVFILT_READ
  EV_ADD EV_CLEAR EV_DELETE
  NOTE_WRITE NOTE_DELETE NOTE_RENAME NOTE_ATTRIB);
our %EXPORT_TAGS = ( constants => \@EXPORT_OK );

sub new {
    bless { kq => new IO::KQueue, last_udata => 42, }, $_[0];
}

sub kevent {
    my ( $self, $timeout ) = @_;
    map {
        my @e = @$_;
        $e[KQ_UDATA] = $self->{ $e[KQ_UDATA] }
          if defined $e[KQ_UDATA] && $self->{ $e[KQ_UDATA] };
        \@e;
    } $self->{kq}->kevent($timeout);
}

sub EV_SET {
    my $self = $_[0];
    my $udata;
    $self->{ $udata = ++$self->{last_udata} } = $_[6] if defined $_[6];
    $self->{kq}->EV_SET( @_[ 1 .. 5 ], $udata );
}

sub delete_objects {
    my ( $self, $filter ) = @_;
    foreach ( grep { /^[0-9]+$/s } keys %$self ) {
        delete $self->{$_} if $filter->( $self->{$_} );
    }
}

1;
