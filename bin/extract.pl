#!/usr/bin/env perl

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
use Carp;
$SIG{__DIE__} = \&Carp::confess;
use Encode 'decode_utf8';
binmode STDERR, ':utf8';
use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename qw(dirname basename);
use Cwd;
my ( $startFolder, $perl5dir );

BEGIN {
    $SIG{INT} = $SIG{USR1} = $SIG{USR2} = sub {
        my ($sig) = @_;
        die "Died on $sig signal\n";
    };
    $startFolder = decode_utf8 getcwd();
    my $homedir = dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
    while (1) {
        $perl5dir = catdir( $homedir, 'lib' );
        last if -d catdir( $perl5dir, 'FileMgt106' );
        my $parent = dirname $homedir;
        last if $parent eq $homedir;
        $homedir = $parent;
    }
    chdir $perl5dir or die "chdir $perl5dir: $!";
    $perl5dir = decode_utf8 getcwd();
    chdir $startFolder;
}
use lib $perl5dir;
use JSON;

my ( $processScal, $processQuery );

foreach (@ARGV) {
    local $_ = decode_utf8 $_;
    if (/^-+nohints/i) {
        require FileMgt106::Extract;
        $processScal =
          FileMgt106::Extract::makeSimpleExtractor(
            FileMgt106::Extract::makeExtractAcceptor(@ARGV) );
        next;
    }
    elsif (/^-+csv=?(.*)/i) {
        require FileMgt106::Extract;
        ( $processScal, $processQuery ) =
          FileMgt106::Extract::makeDataExtractor(
            catfile( dirname($perl5dir), '~$hints' ),
            FileMgt106::Extract::makeCsvWriter($1)
          );
        next;
    }
    elsif (/^-+(xlsx?)=?(.*)/i) {
        require FileMgt106::Extract;
        ( $processScal, $processQuery ) =
          FileMgt106::Extract::makeDataExtractor(
            catfile( dirname($perl5dir), '~$hints' ),
            FileMgt106::Extract::makeSpreadsheetWriter( $1, $2 || 'Extracted' )
          );
        next;
    }
    elsif (/^-+info/i) {
        require FileMgt106::Extract;
        ( $processScal, $processQuery ) =
          FileMgt106::Extract::makeInfoExtractor(
            catfile( dirname($perl5dir), '~$hints' ) );
        next;
    }
    unless ($processScal) {
        my $hintsFile = catfile( dirname($perl5dir), '~$hints' );
        require FileMgt106::Extract;
        if ( grep { /^-+cwd/i } @ARGV ) {
            $processScal = FileMgt106::Extract::makeHintsBuilder($hintsFile);
        }
        elsif ( grep { /^-+filter/i } @ARGV ) {
            $processScal = FileMgt106::Extract::makeHintsFilter($hintsFile);
        }
        else {
            $processScal =
              FileMgt106::Extract::makeHintsExtractor( $hintsFile,
                FileMgt106::Extract::makeExtractAcceptor(@ARGV) );
        }
    }
    if (/^-$/) {
        local undef $/;
        binmode STDIN;
        my $missingCompilation;
        require FileMgt106::Tools;
        FileMgt106::Tools::setNormalisation('win');
        local $_ = <STDIN>;
        foreach (
            eval { decode_json($_); } || map {
                if ( -f $_ && /(?:.*)\.(jbz|json\.bz2|txt|json)$/s ) {
                    warn "Filtering $_";
                    if ( $1 eq 'txt' || $1 eq 'json' ) {
                        open my $fh, '<', $_;
                        binmode $fh;
                        local undef $/;
                        decode_json(<$fh>);
                    }
                    else {
                        FileMgt106::Tools::loadJbz($_);
                    }
                }
                else {
                    warn "Not processed: $_";
                    ();
                }
            } split /[\r\n]+/
          )
        {
            my $missing =
              $processScal->( FileMgt106::Tools::normaliseHash($_) );
            $missingCompilation->{$_} = $missing if $missing;
        }
        if ( $missingCompilation
            && ( my $numKeys = keys %$missingCompilation ) )
        {
            ($missingCompilation) = values %$missingCompilation
              if $numKeys == 1;
            if (undef) {
                binmode STDOUT, ':utf8';
                require JSON;
                print JSON->new->canonical(1)
                  ->pretty->encode($missingCompilation);
            }
            else {
                require FileMgt106::Tools;
                FileMgt106::Tools::saveJbz( "+missing.jbz.$$",
                    $missingCompilation );
                rename "+missing.jbz.$$", '+missing.jbz';
            }
        }
    }
    elsif (/^[0-9a-f]{40}$/is) {
        $processScal->($_);
    }
    elsif ( -f $_ && /(.*)\.(?:jbz|json\.bz2)$/s ) {
        require FileMgt106::Tools;
        if ( my $s = $processScal->( FileMgt106::Tools::loadJbz($_) ) ) {
            s/(\.jbz|json\.bz2)$/+missing$1/s;
            FileMgt106::Tools::saveJbz( $_, $s );
        }
    }
    elsif ($processQuery) {
        $processQuery->($_);
    }
    elsif ( !/^-+(?:cwd|filter|sort|tar|tgz|tbz|newer=.*)$/ ) {
        warn "Ignored: $_";
    }
}
$processScal->() if $processScal;
