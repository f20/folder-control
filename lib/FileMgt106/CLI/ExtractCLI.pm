package FileMgt106::CLI::ExtractCLI;

# Copyright 2011-2019 Franck Latrémolière, Reckon LLP.
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

use Encode qw(decode_utf8);
use File::Basename qw(dirname basename);
use File::Spec::Functions qw(catfile);
use FileMgt106::LoadSave;

use constant {
    STAT_DEV => 0,
    STAT_INO => 1,
};

sub process {

    my ( $startFolder, $perl5dir, @args ) = @_;
    my (
        $catalogueProcessor, $queryProcessor, $resultsProcessor,
        $consolidator,       $missingCompilation
    );
    my $missingStream = \*STDERR;
    my $hintsFile = catfile( dirname($perl5dir), '~$hints' );

    foreach (@args) {

        local $_ = decode_utf8 $_;

        if (/^-+(?:make|build|cwd)(symlink)?(infill)?/i) {
            require FileMgt106::Builder;
            $catalogueProcessor =
              FileMgt106::Builder::makeHintsBuilder( $hintsFile, $1, $2 );
            $missingStream = \*STDOUT;
            next;
        }

        if (/^-+resolve/) {
            require FileMgt106::Database;
            my $hints = FileMgt106::Database->new( $hintsFile, 1 );
            require FileMgt106::ResolveFilter;
            require FileMgt106::Scanner;
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                rename "$path.jbz", "$path+symlinks.jbz";
                FileMgt106::LoadSave::saveJbz(
                    "$path.jbz",
                    FileMgt106::ResolveFilter::resolveAbsolutePaths(
                        $scalar,
                        $hints->{sha1FromStat},
                        \&FileMgt106::Scanner::sha1File
                    )
                );
                return;
            };
            next;
        }

        if (/^-+denest/) {
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                my $denest;
                $denest = sub {
                    my ($s) = @_;
                    my %d;
                    while ( my ( $k, $v ) = each %$s ) {
                      TRYAGAIN: if ( ref $v eq 'HASH' ) {
                            if ( keys %$v == 1 ) {
                                my ( $k2, $v2 ) = %$v;
                                if ( ref $v2 eq 'HASH' ) {
                                    my $kk = "$k, $k2";
                                    $kk .= ',' while exists $d{$kk};
                                    $k = $kk;
                                    $v = $v2;
                                    goto TRYAGAIN;
                                }
                            }
                            $d{$k} = $denest->($v);
                            next;
                        }
                        $d{$k} = $v;
                    }
                    \%d;
                };
                FileMgt106::LoadSave::saveJbz( "$path+denested.jbz",
                    $denest->($scalar) );
                return;
            };
            next;
        }

        if (/^-+split/) {
            $catalogueProcessor = sub {
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
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                my ($module) =
                  grep { s#^/(FilterFactory::)#FileMgt106::$1#; } keys %$scalar;
                if ($module) {
                    undef $module unless eval "require $module";
                    warn $@ if $@;
                }
                require FileMgt106::FilterFactory::ByType unless $module;
                my ( $exploded, $newPath ) =
                    $module
                  ? $module->new($scalar)->explode($path)
                  : FileMgt106::FilterFactory::ByType::explodeByType( $scalar,
                    $path );
                while ( my ( $k, $v ) = each %$exploded ) {
                    FileMgt106::LoadSave::saveJbz( "$newPath \$$k.jbz", $v )
                      if ref $v;
                }
                return;
            };
            next;
        }

        if (/^-+metadatasingle/i) {
            require FileMgt106::Extraction::Metadata;
            $resultsProcessor =
              FileMgt106::Extraction::Metadata::metadataProcessorMaker(
                catfile( dirname($perl5dir), '~$metadata' ) );
            next;
        }

        if (/^-+metadata/i) {
            require FileMgt106::Extraction::Metadata;
            $resultsProcessor =
              FileMgt106::Extraction::Metadata::metadataThreadedProcessorMaker(
                catfile( dirname($perl5dir), '~$metadata' ) );
            next;
        }

        if (/^-+exiftool/i) {
            require FileMgt106::Extraction::Metadata;
            $resultsProcessor =
              FileMgt106::Extraction::Metadata::metadaExtractorMakerSimple();
            next;
        }

        if (/^-+csv=?(.*)/i) {
            require FileMgt106::Extraction::Extractor;
            require FileMgt106::Extraction::Spreadsheets;
            $queryProcessor =
              FileMgt106::Extraction::Extractor::makeDataExtractor( $hintsFile,
                FileMgt106::Extraction::Spreadsheets::makeCsvWriter($1),
                $resultsProcessor );
            next;
        }

        if (/^-+(xlsx?)=?(.*)/i) {
            require FileMgt106::Extraction::Extractor;
            require FileMgt106::Extraction::Spreadsheets;
            $queryProcessor =
              FileMgt106::Extraction::Extractor::makeDataExtractor(
                $hintsFile,
                FileMgt106::Extraction::Spreadsheets::makeSpreadsheetWriter(
                    $1, $2 || 'Extracted'
                ),
                $resultsProcessor
              );
            next;
        }

        if (/^-+info/i) {
            require FileMgt106::Extraction::Extractor;
            ( $catalogueProcessor, $queryProcessor ) =
              FileMgt106::Extraction::Extractor::makeInfoExtractor($hintsFile);
            next;
        }

        if (/^-+nohints/i) {
            require FileMgt106::Extraction::Extractor;
            $catalogueProcessor =
              FileMgt106::Extraction::Extractor::makeSimpleExtractor(
                FileMgt106::Extraction::Extractor::makeExtractAcceptor(@args) );
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
            die 'Cannot find any file system for filtering' unless $devNo;
            require FileMgt106::HintsFilter;
            $catalogueProcessor =
              FileMgt106::HintsFilter::makeHintsFilter( $hintsFile,
                $devNo, $devOnly );
            $missingStream = \*STDOUT;
            next;
        }

        if (/^-+base/i) {
            require FileMgt106::ConsolidateFilter;
            $consolidator ||= FileMgt106::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->baseProcessor;
            next;
        }
        if (/^-+(add|new)/i) {
            require FileMgt106::ConsolidateFilter;
            $consolidator ||= FileMgt106::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->additionsProcessor;
            next;
        }
        if (/^-+dup/i) {
            require FileMgt106::ConsolidateFilter;
            $consolidator ||= FileMgt106::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->duplicationsProcessor;
            next;
        }

        unless ($catalogueProcessor) {
            require FileMgt106::Extraction::Extractor;
            $catalogueProcessor =
              FileMgt106::Extraction::Extractor::makeHintsExtractor( $hintsFile,
                FileMgt106::Extraction::Extractor::makeExtractAcceptor(@args) );
        }

        if (/^-$/) {
            local undef $/;
            binmode STDIN;
            my $stdinblob = <STDIN>;
            if (
                my $stdinscalar = eval {
                    FileMgt106::LoadSave::jsonMachineMaker()
                      ->decode($stdinblob);
                }
              )
            {
                $missingCompilation = $catalogueProcessor->($stdinscalar);
            }
            else {
                foreach ( split /[\r\n]+/, $stdinblob ) {
                    if ( -f $_ && /(.*)\.(jbz|json\.bz2|txt|json)$/s ) {
                        if ( $2 eq 'txt' || $2 eq 'json' ) {
                            open my $fh, '<', $_;
                            binmode $fh;
                            local undef $/;
                            tr#/#|#;
                            my $missing = $catalogueProcessor->(
                                FileMgt106::LoadSave::jsonMachineMaker()
                                  ->decode(<$fh>),
                                $1
                            );
                            $missingCompilation->{$1} = $missing
                              if $missing;
                        }
                        else {
                            my $scalar =
                              FileMgt106::LoadSave::loadNormalisedScalar($_);
                            tr#/#|#;
                            my $missing = $catalogueProcessor->( $scalar, $1 );
                            $missingCompilation->{$1} = $missing
                              if $missing;
                        }
                    }
                    else {
                        warn "Not processed: $_";
                    }
                }
            }
        }

        elsif ( -f $_ && /(.*)\.(jbz|json\.bz2|txt|json)$/s ) {
            my $missing;
            if ( $2 eq 'txt' || $2 eq 'json' ) {
                open my $fh, '<', $_;
                binmode $fh;
                local undef $/;
                tr#/#|#;
                $missing = $catalogueProcessor->(
                    FileMgt106::LoadSave::jsonMachineMaker()->decode(<$fh>), $1
                );
            }
            else {
                my $scalar = FileMgt106::LoadSave::loadNormalisedScalar($_);
                tr#/#|#;
                $missing = $catalogueProcessor->( $scalar, $1 );
            }
            $missingCompilation->{$1} = $missing if $missing;
        }

        elsif (/^[0-9a-f]{40}$/is) {
            $catalogueProcessor->($_);
        }

        elsif ($queryProcessor) {
            $queryProcessor->($_);
        }

        elsif ( !/^-+(?:sort|tar|tgz|tbz|newer=.*)$/ ) {
            warn "Ignored: $_";
        }

    }

    if ($missingCompilation) {
        binmode $missingStream;
        print {$missingStream}
          FileMgt106::LoadSave::jsonMachineMaker()->encode($missingCompilation);
    }

    $catalogueProcessor->() if $catalogueProcessor;

}

1;
