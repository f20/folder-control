package FileMgt106::CLI::ExtractCLI;

=head Copyright licence and disclaimer

Copyright 2011-2017 Franck Latrémolière, Reckon LLP.

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
use FileMgt106::LoadSave;

use constant { STAT_DEV => 0, };

sub process {

    my ( $startFolder, $perl5dir, @args ) = @_;
    my ( $processScal, $processQuery, $extraColumnExtractor );
    my $hintsFile = catfile( dirname($perl5dir), '~$hints' );

    foreach (@args) {

        local $_ = decode_utf8 $_;

        if (/^-+nohints/i) {
            require FileMgt106::Extractor;
            $processScal =
              FileMgt106::Extractor::makeSimpleExtractor(
                FileMgt106::Extractor::makeExtractAcceptor(@args) );
            next;
        }

        if (/^-+split/) {
            $processScal = sub {
                my ( $scalar, $path ) = @_ or return;
                while ( my ( $k, $v ) = each %$scalar ) {
                    local $_ = $k;
                    s#/#..#g;
                    FileMgt106::LoadSave::saveJbz( "$path \$$_.jbz",
                        ref $v ? $v : { $k => $v } );
                }
                return;
            };
            next;
        }

        if (/^-+explode/) {
            $processScal = sub {
                my ( $scalar, $path ) = @_ or return;
                my ($module) =
                  grep { s#^/(FilterFactory::)#FileMgt106::$1#; } keys %$scalar;
                if ($module) {
                    undef $module unless eval "require $module";
                    warn $@ if $@;
                }
                my $exploded =
                    $module
                  ? $module->new($scalar)->exploded
                  : (
                    require FileMgt106::FilterFactory::ByType,
                    FileMgt106::FilterFactory::ByType::explodeByType($scalar)
                  );
                $path =~ s/\.aplibrary$//s;
                while ( my ( $k, $v ) = each %$exploded ) {
                    FileMgt106::LoadSave::saveJbz( "$path \$$k.jbz", $v )
                      if ref $v;
                }
                return;
            };
            next;
        }

        if (/^-+exiftool/i) {
            require Image::ExifTool;
            my $et = Image::ExifTool->new;
            my @tags =
              qw(SerialNumber ShutterCount DateTimeOriginal ImageWidth ImageHeight);
            $extraColumnExtractor = sub {
                my ($fn) = @_;
                return @tags
                  unless defined $fn;
                $et->ExtractInfo($fn) or return map { ['']; } @tags;
                my $info = $et->GetInfo(@tags);
                map { defined $_ ? [$_] : ['']; } @$info{@tags};
            };
            next;
        }

        if (/^-+csv=?(.*)/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeDataExtractor( $hintsFile,
                FileMgt106::Extractor::makeCsvWriter($1),
                $extraColumnExtractor );
            next;
        }

        if (/^-+(xlsx?)=?(.*)/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeDataExtractor(
                $hintsFile,
                FileMgt106::Extractor::makeSpreadsheetWriter(
                    $1, $2 || 'Extracted'
                ),
                $extraColumnExtractor
              );
            next;
        }

        if (/^-+info/i) {
            require FileMgt106::Extractor;
            ( $processScal, $processQuery ) =
              FileMgt106::Extractor::makeInfoExtractor($hintsFile);
            next;
        }

        if (/^-+cwd(symlink)?(infill)?/i) {
            require FileMgt106::Extractor;
            $processScal =
              FileMgt106::Extractor::makeHintsBuilder( $hintsFile, $1, $2 );
            next;
        }

        if (/^-+filter=?(.*)/i) {
            my ( $devNo, $devOnly );
            if ($1) {
                if ( my @stat = stat $1 ) {
                    $devNo   = $stat[STAT_DEV];
                    $devOnly = 1;
                }
            }
            elsif ( my @stat = stat $hintsFile ) {
                $devNo = $stat[STAT_DEV];
            }
            if ($devNo) {
                require FileMgt106::Extractor;
                $processScal =
                  FileMgt106::Extractor::makeHintsFilter( $hintsFile,
                    $devNo, $devOnly );
            }
            else {
                $processScal = FileMgt106::LoadSave::makeInfillFilter();
            }
            next;
        }

        unless ($processScal) {
            require FileMgt106::Extractor;
            $processScal =
              FileMgt106::Extractor::makeHintsExtractor( $hintsFile,
                FileMgt106::Extractor::makeExtractAcceptor(@args) );
        }

        if (/^-$/) {
            local undef $/;
            binmode STDIN;
            my $missingCompilation;
            my $stdin = <STDIN>;
            foreach (
                eval {
                    FileMgt106::LoadSave::jsonMachineMaker()->decode($stdin);
                } || map {
                    if ( -f $_ && /(?:.*)\.(jbz|json\.bz2|txt|json)$/s ) {
                        warn "Filtering $_";
                        if ( $1 eq 'txt' || $1 eq 'json' ) {
                            open my $fh, '<', $_;
                            binmode $fh;
                            local undef $/;
                            tr#/#|#;
                            +{ $_ =>
                                  FileMgt106::LoadSave::jsonMachineMaker()
                                  ->decode(<$fh>) };
                        }
                        else {
                            my $scalar =
                              FileMgt106::LoadSave::loadNormalisedScalar($_);
                            tr#/#|#;
                            +{ $_ => $scalar };
                        }
                    }
                    else {
                        warn "Not processed: $_";
                        ();
                    }
                } split /[\r\n]+/,
                $stdin
              )
            {
                my $missing = $processScal->($_);
                if ($missing) {
                    if ( !$missingCompilation ) {
                        $missingCompilation = $missing;
                    }
                    elsif ( keys %$missing == 1 ) {
                        my ( $k, $v ) = %$missing;
                        $missingCompilation->{"$k $_"} = $v;
                    }
                    else { $missingCompilation->{$_} = $missing; }
                }
            }
            unlink '+missing.jbz';
            FileMgt106::LoadSave::saveJbz( '+missing.jbz', $missingCompilation )
              if $missingCompilation;
        }
        elsif (/^[0-9a-f]{40}$/is) {
            $processScal->($_);
        }
        elsif ( -f $_ && /(.*)\.(?:jbz|json\.bz2)$/s ) {
            my $s = $processScal->(
                FileMgt106::LoadSave::loadNormalisedScalar($_), $1
            );
            s/(\.jbz|json\.bz2)$/+missing$1/s;
            unlink $_;
            FileMgt106::LoadSave::saveJbz( $_, $s ) if $s;
        }
        elsif ($processQuery) {
            $processQuery->($_);
        }
        elsif ( !/^-+(?:sort|tar|tgz|tbz|newer=.*)$/ ) {
            warn "Ignored: $_";
        }
    }

    $processScal->() if $processScal;

}

1;
