package Daemon112::TestWatch;

=head Copyright licence and disclaimer

Copyright 2013-2016 Franck Latrémolière.

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

use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename 'dirname';
use Cwd;
use Encode 'decode_utf8';
use FileMgt106::Database;
use Daemon112::TopMaster;
require Daemon112::Watcher;    # used but not loaded by TopMaster

sub new {
    my ( $class, $qu, $pq, $kq, $home ) = @_;
    mkdir $home = catdir( $home, 'testarea.tmp' );
    mkdir my $git = catdir( $home, 'git' );
    chdir $git && `git init`;
    mkdir my $jbz  = catdir( $home, 'jbz' );
    mkdir my $repo = catdir( $home, 'repo' );
    chdir $repo && `git init`;
    mkdir my $top = catdir( $home, 'top' );
    mkdir catdir( $top, 'mid' );
    mkdir catdir( $top, 'test1' );
    mkdir catdir( $top, 'mid', 'test2' );
    bless {
        # The following fields are used by TopMaster
        hints => FileMgt106::Database->new( catfile( $home, '~$hints' ) ),
        kq    => $kq,
        pq    => $pq,
        qu    => $qu,
        locs => { repo => $repo, git => $git, jbz => $jbz },

        # The following fields are private
        top => $top,
    }, $class;
}

sub dumpState {
    my ($self) = @_;
    warn $self . "->{$_} = $self->{$_}\n" foreach sort keys %$self;
    $self->{topMaster}->dumpState
      if UNIVERSAL::can( $self->{topMaster}, 'dumpState' );
}

sub start {
    my ($self) = @_;
    $self->{qu}->enqueue(
        time,
        $self->{topMaster} ||= Daemon112::TopMaster->new(
            '/kq' => $self->{kq},
            '/pq' => $self->{pq},
            'mid' => Daemon112::TopMaster->new(
                '/kq' => $self->{kq},
                '/pq' => $self->{pq},
            ),
        )->attach( $self->{top} )
    );
    $self;
}

1;
