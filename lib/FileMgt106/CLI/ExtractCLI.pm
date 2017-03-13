package FileMgt106::CLI::ExtractCLI;

=head Copyright licence and disclaimer

Copyright 2011-2016 Franck Latrémolière, Reckon LLP.

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

use Encode qw(decode_utf8);
use File::Basename qw(dirname basename);
use File::Spec::Functions qw(catfile);
use JSON;
use FileMgt106::LoadSave;

sub process {

    my ( $startFolder, $perl5dir, @args ) = @_;

    my ( $processScal, $processQuery );

    foreach (@args) {
        local $_ = decode_utf8 $_;
        if (/^-+nohints/i) {
            require FileMgt106::Extractor;
            $processScal =
              FileMgt106::Extractor::makeSimpleExtractor(
                FileMgt106::Extractor::makeExtractAcceptor(@args) );
            next;
        }
        elsif (/^-+csv=?(.*)/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeDataExtractor(
                catfile( dirname($perl5dir), '~$hints' ),
                FileMgt106::Extractor::makeCsvWriter($1)
              );
            next;
        }
        elsif (/^-+(xlsx?)=?(.*)/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeDataExtractor(
                catfile( dirname($perl5dir), '~$hints' ),
                FileMgt106::Extractor::makeSpreadsheetWriter(
                    $1, $2 || 'Extracted'
                )
              );
            next;
        }
        elsif (/^-+info/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeInfoExtractor(
                catfile( dirname($perl5dir), '~$hints' ) );
            next;
        }
        unless ($processScal) {
            my $hintsFile = catfile( dirname($perl5dir), '~$hints' );
            require FileMgt106::Extractor;
            if ( grep { /^-+cwd/i } @args ) {
                $processScal =
                  FileMgt106::Extractor::makeHintsBuilder($hintsFile);
            }
            elsif ( grep { /^-+filter/i } @args ) {
                $processScal =
                  FileMgt106::Extractor::makeHintsFilter($hintsFile);
            }
            else {
                $processScal =
                  FileMgt106::Extractor::makeHintsExtractor( $hintsFile,
                    FileMgt106::Extractor::makeExtractAcceptor(@args) );
            }
        }
        if (/^-$/) {
            local undef $/;
            binmode STDIN;
            my $missingCompilation;
            FileMgt106::LoadSave::setNormalisation('win');
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
                            FileMgt106::LoadSave::loadNormalisedScalar($_);
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
                  $processScal->( FileMgt106::LoadSave::normaliseHash($_) );
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
                    FileMgt106::LoadSave::saveJbz( "+missing.jbz.$$",
                        $missingCompilation );
                    rename "+missing.jbz.$$", '+missing.jbz';
                }
            }
        }
        elsif (/^[0-9a-f]{40}$/is) {
            $processScal->($_);
        }
        elsif ( -f $_ && /(.*)\.(?:jbz|json\.bz2)$/s ) {
            if (
                my $s = $processScal->(
                    FileMgt106::LoadSave::loadNormalisedScalar($_)
                )
              )
            {
                s/(\.jbz|json\.bz2)$/+missing$1/s;
                FileMgt106::LoadSave::saveJbz( $_, $s );
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

}

1;
