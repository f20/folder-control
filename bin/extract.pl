#!/usr/bin/env perl

# Copyright 2011-2017 Franck Latrémolière, Reckon LLP and others.
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
use Cwd qw(getcwd);
use Encode qw(decode_utf8);
use File::Basename qw(dirname);
use File::Spec::Functions qw(catdir rel2abs);
binmode STDERR, ':utf8';
my ( $startFolder, $perl5dir, @otherLibs );

BEGIN {
    $startFolder = decode_utf8 getcwd();
    my $homedir = dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
    while (1) {
        $perl5dir = catdir( $homedir, 'lib' );
        last if -d catdir( $perl5dir, 'FileMgt106' );
        my $parent = dirname $homedir;
        last if $parent eq $homedir;
        $homedir = $parent;
    }
    my $cpanLib = catdir( $homedir, 'cpan' );
    push @otherLibs, $cpanLib if -d $cpanLib;
    chdir $perl5dir or die "chdir $perl5dir: $!";
    $perl5dir = decode_utf8 getcwd();
    chdir $startFolder if defined $startFolder;
}
use lib @otherLibs, $perl5dir;

use FileMgt106::CLI::ExtractCLI;
FileMgt106::CLI::ExtractCLI::process(
    $startFolder,
    $perl5dir,
    grep {
        /^-+carp$/s
          ? ( require Carp, $SIG{__DIE__} = \&Carp::confess, undef )
          : 1;
    } @ARGV
);
