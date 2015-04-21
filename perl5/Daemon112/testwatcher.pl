#!/usr/bin/env perl

=head Copyright licence and disclaimer

Copyright 2015 Franck Latrémolière.

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
use Carp;
$SIG{__DIE__} = \&Carp::confess;
binmode STDERR, ':utf8';
use Encode 'decode_utf8';

use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename qw(dirname basename);
use Cwd;
my ( $startFolder, $perl5dir );

BEGIN {
    $SIG{INT} = $SIG{USR1} = $SIG{USR2} = sub {
        my ($sig) = @_;
        die "Died on $sig signal\n";
    };
    $startFolder = getcwd();
    $perl5dir = dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
    while (1) {
        last if -d catdir( $perl5dir, 'FileMgt106' );
        my $parent = dirname $perl5dir;
        last if $parent eq $perl5dir;
        $perl5dir = $parent;
    }
    chdir $perl5dir or die "chdir $perl5dir: $!";
    $perl5dir = getcwd();
    chdir $startFolder;
}
use lib $perl5dir;
mkdir catdir( dirname($perl5dir), '~$' );
mkdir catdir( dirname($perl5dir), 'top' );
mkdir catdir( dirname($perl5dir), 'top', 'test' );
use Daemon112::Daemon;
Daemon112::Daemon->run('Daemon112::TestWatch');
