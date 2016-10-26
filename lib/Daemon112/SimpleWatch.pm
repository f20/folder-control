package Daemon112::SimpleWatch;

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

use File::Spec::Functions qw(catfile catdir);
use File::Basename 'dirname';
use Cwd;
use Encode 'decode_utf8';
use FileMgt106::Database;
use Daemon112::TopMaster;
require Daemon112::Watcher;    # used but not loaded by TopMaster

sub new {
    my ( $class, $qu, $pq, $kq, $hintsFile, $top, $repo, $git, $jbz, $parent )
      = @_;
    warn 'Daemon112::SimpleWatch started with'
      . '$hintsFile'
      . ( defined $hintsFile ? " = $hintsFile" : ' undefined' )
      . '; $top'
      . ( defined $top ? " = $top" : ' undefined' )
      . '; $repo'
      . ( defined $repo ? " = $repo" : ' undefined' )
      . '; $git'
      . ( defined $git ? " = $git" : ' undefined' )
      . '; $jbz'
      . ( defined $jbz ? " = $jbz" : ' undefined' )
      . '; $parent'
      . ( defined $parent ? " = $parent" : ' undefined' );
    my @extras;
    if ( !$hintsFile && $parent ) {
        mkdir my $home = catdir( $parent, 'testarea.tmp' );
        $hintsFile = catfile( $home, '~$hints' );
        mkdir $git = catdir( $home, 'git' );
        chdir $git && `git init`;
        mkdir $jbz  = catdir( $home, 'jbz' );
        mkdir $repo = catdir( $home, 'repo' );
        mkdir $top  = catdir( $home, 'top' );
        mkdir catdir( $top, 'mid' );
        @extras =
          ( 'mid' => Daemon112::TopMaster->new( '/kq' => $kq, '/pq' => $pq, ) );
        mkdir my $test1 = catdir( $top, 'test1' );
        mkdir my $test2 = catdir( $top, 'mid', 'test2' );
        my $makeRandoms;
        $makeRandoms = sub {
            my $file = rand();
            chdir $test1;
            `dd if=/dev/urandom of=test1-$file count=1`;
            chdir $test2;
            `dd if=/dev/urandom of=test2-$file count=1`;
            $qu->enqueue( time + 8, $makeRandoms );
        };
        $pq->enqueue( time + 10, $makeRandoms );
    }
    bless {
        # The following fields are used by TopMaster and others
        hints => FileMgt106::Database->new($hintsFile),
        kq    => $kq,
        pq    => $pq,
        qu    => $qu,
        locs  => { repo => $repo, git => $git, jbz => $jbz },

        # The following field is private
        topMaster =>
          Daemon112::TopMaster->new( '/kq' => $kq, '/pq' => $pq, @extras )
          ->attach($top),
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
    $self->{qu}->enqueue( time, $self->{topMaster} );
    $self;
}

1;
