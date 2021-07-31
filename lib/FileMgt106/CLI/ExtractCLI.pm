package FileMgt106::CLI::ExtractCLI;

# Copyright 2011-2021 Franck Latrémolière and others.
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
use File::Spec::Functions qw(catdir catfile);
use FileMgt106::Catalogues::LoadSaveNormalize;

use constant {
    STAT_DEV => 0,
    STAT_INO => 1,
};

sub process {

    my ( $startFolder, $perl5dir, @args ) = @_;
    my ( $catalogueProcessor, $queryProcessor, $consolidator, $outputScalar );
    my $outputStream = \*STDERR;
    my $hintsFile = catfile( dirname($perl5dir), '~$hints' );

    foreach (@args) {

        local $_ = decode_utf8 $_;

        if (/^-+(?:make|build|cwd)(symlink)?(infill)?/i) {
            require FileMgt106::Folders::Builder;
            $catalogueProcessor =
              FileMgt106::Folders::Builder::makeHintsBuilder( $hintsFile, $1,
                $2 );
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+mailbox(archive)?/) {
            require EmailMgt108::MailboxTools;
            $catalogueProcessor =
              EmailMgt108::MailboxTools::makeMailboxProcessor( $hintsFile, $1 );
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+resolve/) {
            require FileMgt106::Database;
            my $hints = FileMgt106::Database->new( $hintsFile, 1 );
            require FileMgt106::Scanning::ResolveFilter;
            require FileMgt106::Scanning::Scanner;
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                rename "$path.jbz", "$path+symlinks.jbz";
                FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
                    "$path.jbz",
                    FileMgt106::Scanning::ResolveFilter::resolveAbsolutePaths(
                        $scalar,
                        $hints->{sha1FromStat},
                        \&FileMgt106::Scanning::Scanner::sha1File
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
                FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
                    "$path+denested.jbz", $denest->($scalar) );
                return;
            };
            next;
        }

        if (/^-+split/) {
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                $path = '' unless defined $path;
                while ( my ( $k, $v ) = each %$scalar ) {
                    local $_ = $k;
                    s#/#..#g;
                    FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
                        "$path \$$_.jbz",
                        ref $v ? $v : { $k => $v } );
                }
                return;
            };
            next;
        }

        if (/^-+explode/) {
            my $byExtensionFlag = /ext/;
            $catalogueProcessor = sub {
                my ( $scalar, $path ) = @_ or return;
                $path = 'STDIN' unless defined $path;
                my ($module) =
                  grep { s#^/(FilterFactory::)#FileMgt106::$1#; } keys %$scalar;
                if ($module) {
                    undef $module unless eval "require $module";
                    warn $@ if $@;
                }
                require FileMgt106::FilterFactory::ByType unless $module;
                my ( $exploded, $newPath ) =
                  $module            ? $module->new($scalar)->explode($path)
                  : $byExtensionFlag ? (
                    FileMgt106::FilterFactory::ByType::explodeByExtension(
                        $scalar),
                    $path
                  )
                  : (
                    FileMgt106::FilterFactory::ByType::explodeByType($scalar),
                    $path
                  );
                while ( my ( $k, $v ) = each %$exploded ) {
                    FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
                        "$newPath \$$k.jbz", $v )
                      if ref $v;
                }
                return;
            };
            next;
        }

        if (/^-+stats?/i) {
            require FileMgt106::Extraction::Statistics;
            $catalogueProcessor =
              FileMgt106::Extraction::Statistics->makeStatisticsExtractor(
                $hintsFile);
            next;
        }

        if (/-+metadata(basic|simple)?/i) {
            require FileMgt106::Extraction::MetadataReports;
            $catalogueProcessor =
              $1
              ? FileMgt106::Extraction::MetadataReports->makeFiledataExtractor(
                $hintsFile)
              : FileMgt106::Extraction::MetadataReports->makeMetadataExtractor(
                $hintsFile, catfile( dirname($hintsFile), '~$metadata' ) );
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
            require FileMgt106::Catalogues::HintsFilter;
            $catalogueProcessor =
              FileMgt106::Catalogues::HintsFilter::makeHintsFilter( $hintsFile,
                $devNo, $devOnly );
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+find=?(.+)/i) {
            require FileMgt106::Catalogues::FindFilter;
            $catalogueProcessor =
              FileMgt106::Catalogues::FindFilter->processor($1);
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+dups/i) {
            require FileMgt106::Catalogues::ConsolidateFilter;
            $catalogueProcessor =
              FileMgt106::Catalogues::ConsolidateFilter
              ->duplicationsByPairProcessor;
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+(?:consol|merge)/i) {
            require FileMgt106::Catalogues::ConsolidateFilter;
            $catalogueProcessor =
              FileMgt106::Catalogues::ConsolidateFilter->consolidateProcessor;
            $outputStream = \*STDOUT;
            next;
        }

        if (/^-+base/i) {
            require FileMgt106::Catalogues::ConsolidateFilter;
            $consolidator ||= FileMgt106::Catalogues::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->baseProcessor;
            next;
        }

        if (/^-+(?:new|unseen)/i) {
            require FileMgt106::Catalogues::ConsolidateFilter;
            $consolidator ||= FileMgt106::Catalogues::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->unseenProcessor;
            $outputStream       = \*STDOUT;
            next;
        }

        if (/^-+(?:dup|seen)/i) {
            require FileMgt106::Catalogues::ConsolidateFilter;
            $consolidator ||= FileMgt106::Catalogues::ConsolidateFilter->new;
            $catalogueProcessor = $consolidator->seenProcessor;
            $outputStream       = \*STDOUT;
            next;
        }

        if (/^-+nohints/i) {
            require FileMgt106::Extraction::Extractor;
            $catalogueProcessor =
              FileMgt106::Extraction::Extractor::makeSimpleExtractor(
                FileMgt106::Extraction::Extractor::makeExtractAcceptor(@args) );
            next;
        }

        if (/^-+(?:sort|tar|tgz|tbz)$/) {
            require FileMgt106::Extraction::Extractor;
            $catalogueProcessor =
              FileMgt106::Extraction::Extractor::makeHintsExtractor( $hintsFile,
                FileMgt106::Extraction::Extractor::makeExtractAcceptor($_) );
            next;
        }

        if (/^-+info/i) {
            require FileMgt106::Extraction::Extractor;
            ( $catalogueProcessor, $queryProcessor ) =
              FileMgt106::Extraction::Extractor::makeInfoExtractor($hintsFile);
            next;
        }

        if (/^-+(csv|xlsx?)(metadata(single)?)?=?(.*)/i) {
            require FileMgt106::Extraction::Extractor;
            require FileMgt106::Extraction::Spreadsheets;
            $queryProcessor =
              FileMgt106::Extraction::Extractor::makeDataExtractor(
                $hintsFile,
                FileMgt106::Extraction::Spreadsheets::makeSpreadsheetWriter(
                    $1, $4
                ),
                $2
                ? do {
                    require FileMgt106::Extraction::MetadataReports;
                    FileMgt106::Extraction::MetadataReports
                      ->makeMetadataWideProcessor(
                        catfile( dirname($hintsFile), '~$metadata' ),
                        $3 ? 1 : undef );
                  }
                : undef,
              );
            next;
        }

        if ($queryProcessor) {
            $queryProcessor->($_);
            next;
        }

        unless ($catalogueProcessor) {
            require FileMgt106::Extraction::Extractor;
            $catalogueProcessor =
              FileMgt106::Extraction::Extractor::makeHintsExtractor( $hintsFile,
                FileMgt106::Extraction::Extractor::makeExtractAcceptor() );
        }

        if (/^-$/) {
            local undef $/;
            binmode STDIN;
            my $stdinblob = <STDIN>;
            if (
                my $stdinscalar = eval {
                    FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker(
                    )->decode($stdinblob);
                }
              )
            {
                $outputScalar = $catalogueProcessor->($stdinscalar);
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
                                FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker(
                                  )->decode(<$fh>),
                                $1
                            );
                            $outputScalar->{$1} = $missing if $missing;
                        }
                        else {
                            my $scalar =
                              FileMgt106::Catalogues::LoadSaveNormalize::loadNormalisedScalar(
                                $_);
                            tr#/#|#;
                            my $missing = $catalogueProcessor->( $scalar, $1 );
                            $outputScalar->{$1} = $missing if $missing;
                        }
                    }
                    else {
                        warn "Not processed: $_";
                    }
                }
            }
        }

        elsif ( my ( $gitRepo, $gitBranch ) = /git:(.+):(\S+)/ ) {
            if ( -d ( my $gitRepo2 = catdir( $gitRepo, '.git' ) ) ) {
                $gitRepo = $gitRepo2;
            }
            my $jsonMachine =
              FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker();
            local $/ = "\000";
            open my $gh,
              qq^git --git-dir="$gitRepo" ls-tree -z -r $gitBranch |^;
            while (<$gh>) {
                my ( $sha1, $name ) = /^\S+ blob (\S+)\t(.*)\000$/s
                  or next;
                $name = decode_utf8 $name;
                open my $h, qq^git --git-dir="$gitRepo" show $sha1 |^;
                binmode $h;
                my $missing =
                  $catalogueProcessor->( $jsonMachine->decode(<$h>), $name );
                $outputScalar->{$name} = $missing if $missing;
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
                    FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker(
                      )->decode(<$fh>),
                    $1
                );
            }
            else {
                my $scalar =
                  FileMgt106::Catalogues::LoadSaveNormalize::loadNormalisedScalar(
                    $_);
                tr#/#|#;
                $missing = $catalogueProcessor->( $scalar, $1 );
            }
            $outputScalar->{$1} = $missing if $missing;
        }

        elsif (/^[0-9a-f]{40}$/is) {
            $catalogueProcessor->($_);
        }

        else {
            warn "Ignored: $_";
        }

    }

    if ($catalogueProcessor) {
        my $processorOutput = $catalogueProcessor->();
        $outputScalar ||= $processorOutput;
    }

    if ($outputScalar) {
        binmode $outputStream;
        print {$outputStream}
          FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker()
          ->encode($outputScalar);
    }

}

1;
