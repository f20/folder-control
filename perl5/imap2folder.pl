#!/usr/bin/env perl

=head Copyright licence and disclaimer

Copyright 2012 Franck Latrémolière, Reckon LLP.

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
use JSON;
require POSIX;

my @dirHandles = map {
    my $h;
    opendir( $h, $_ ) ? $h : ();
} @ARGV;

POSIX::setgid(6);
POSIX::setuid(60);
POSIX::setsid();

use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename 'dirname';
use lib dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
require EmailMgt108::EmailParser;

foreach (@dirHandles) {
    chdir $_ or next;
    open my $scanningHandle, '>', '~$ email index $~/scanning' or next;
    my ( $unstashedFolders, $toScan );
    if ( open my $h, '<', '~$ email index $~/unstashed.json' ) {
        binmode $h;
        local undef $/;
        $unstashedFolders = decode_json(<$h>);
    }
    if ( open my $h, '<', '~$ email index $~/toscan.json' ) {
        binmode $h;
        local undef $/;
        $toScan = decode_json(<$h>);
    }
    while ( my ( $sha1hex, $emailFile ) = each %$toScan ) {
        eval {
            $unstashedFolders->{$sha1hex} =
              EmailMgt108::EmailParser::parseMessage($emailFile);
        };
        warn "$emailFile: $@" if $@;
    }
    binmode $scanningHandle;
    print {$scanningHandle} encode_json($unstashedFolders);
    close $scanningHandle;
    rename '~$ email index $~/scanning', '~$ email index $~/unstashed.json';
    unlink '~$ email index $~/toscan.json';
}
