package FileMgt106::CLI::ScanCLI;

# Copyright 2011-2020 Franck Latrémolière and others.
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
use utf8;

require POSIX;
use Cwd qw(getcwd);
use Encode qw(decode_utf8);
use File::Basename qw(dirname basename);
use File::Spec::Functions qw(catdir catfile rel2abs);
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_GID STAT_MTIME);
use FileMgt106::Catalogues::LoadSaveNormalize;
use FileMgt106::Scanning::ScanMaster;
use FileMgt106::Scanning::Scanner;

sub makeProcessor {

    my ( $self, %options ) = @_;
    my ( $hints, @grabSources, $missing, %scanners, $cleaningFlag,
        $syncDestination, @toRestamp );
    push @grabSources, @{ $options{grabSources} } if $options{grabSources};

    my $scalarAcceptor = sub {
        my ( $scalar, $path, $fileExtension, $targetStatRef, $options ) = @_;
        push @toRestamp, $path if $options->{restamp};
        $hints ||= $self->hintsObj;
        delete $scalar->{$_} foreach grep { /\//; } keys %$scalar;
        my $dir  = $path;
        my $rgid = $targetStatRef->[STAT_GID];
        mkdir $dir unless -e $dir;
        chown 0, $rgid, $dir;
        chmod 0770, $dir;
        chdir $dir or die "chdir $dir: $!";
        $dir = decode_utf8 getcwd();
        my $message = $fileExtension ? " and $fileExtension file" : '';
        warn "Rebuilding $dir with rgid=$rgid$message\n";
        $hints->beginInteractive;
        eval {
            (
                $scanners{$dir} = FileMgt106::Scanning::Scanner->new(
                    $dir, $hints, $self->fileSystemObj->statFromGid($rgid)
                )
              )->scan(
                0,
                $scalar,
                $options->{stash}
                ? [ $options->{stash}, 'Y_Cellar ' . basename($dir) ]
                : (),
              );
        };
        warn "scan $dir: $@" if $@;
        $hints->commit;
        utime time, $targetStatRef->[STAT_MTIME], $dir;
        $missing->{$dir} = $scalar if ref $scalar && keys %$scalar;
    };

    my $folderAcceptor = sub {
        my (@scanMasterCliConfigClosures) = @_;
        $hints ||= $self->hintsObj;
        my $dir = decode_utf8 getcwd();
        if ($syncDestination) {
            my $destination = catdir( $syncDestination, basename($dir) );
            mkdir $destination;
            my ( @extrasSource, @extrasDestination );
            my ($s) =
              FileMgt106::Scanning::Scanner->new( $dir, $hints, @extrasSource )
              ->scan;
            FileMgt106::Scanning::Scanner->new( $destination, $hints,
                @extrasDestination )->scan( 0, $s );
            $hints->commit;
            return;
        }
        if ($cleaningFlag) {
            if ( $cleaningFlag =~ /dayfolder/i ) {
                warn "One folder per day for files in $dir";
                require FileMgt106::Folders::FolderOrganise;
                FileMgt106::Folders::FolderOrganise::categoriseByDay($dir);
            }
            if ( $cleaningFlag =~ /datemark/i ) {
                warn "Datemarking $dir";
                require FileMgt106::Folders::FolderOrganise;
                FileMgt106::Folders::FolderOrganise::datemarkFolder($dir);
            }
            if ( $cleaningFlag =~ /restamp/i ) {
                warn "Re-timestamping $dir";
                require FileMgt106::Folders::FolderOrganise;
                FileMgt106::Folders::FolderOrganise::restampFolder($dir);
            }
            if ( $cleaningFlag =~ /flat/i ) {
                warn "Flattening $dir";
                require FileMgt106::Folders::FolderOrganise;
                $hints->beginInteractive(1);
                FileMgt106::Folders::FolderOrganise::flattenCwd();
                $hints->commit;
            }
            if ( $cleaningFlag =~ /rename/i ) {
                warn "Renaming in $dir";
                $hints->beginInteractive(1);
                FileMgt106::Catalogues::LoadSaveNormalize::renameFilesToNormalisedScannable(
                    '.');
                $hints->commit;
            }
            elsif ( $cleaningFlag =~ /clean/i ) {
                warn "Deep cleaning $dir";
                require FileMgt106::Folders::FolderClean;
                $hints->beginInteractive(1);
                FileMgt106::Folders::FolderClean::deepClean('.');
                $hints->commit;
            }
            return if $cleaningFlag =~ /only/i;
        }
        my $scanMaster =
          FileMgt106::Scanning::ScanMaster->new( $hints, $dir,
            $self->fileSystemObj );
        $_->( $scanMaster, $dir ) foreach @scanMasterCliConfigClosures;
        $scanMaster->dequeued;
    };

    my $finisher = sub {
        if ($missing) {
            if ( grep { $_; } @grabSources ) {
                my @rmdirList;
                $hints ||= $self->hintsObj;
              SOURCE: foreach (@grabSources) {
                    my $grabSource = $_;    # true copy, not loop alias variable
                    my ( $cellarScanner, $cellarDir );
                    if ( $grabSource ne 'done' ) {
                        $cellarDir = $self->homePath;
                        if ( -d ( my $d = $cellarDir . '/Grab.tmp' ) ) {
                            $cellarDir = $d;
                        }
                        {
                            my $toGrab =
                                $grabSource =~ s/:\+$//s
                              ? $missing
                              : _filterByFileName($missing);
                            next SOURCE unless %$toGrab;
                            warn "Grabbing from $grabSource\n";
                            my ( $host, $extract ) =
                              $grabSource =~ /^([a-zA-Z0-9._-]+)$/s
                              ? ( $1, 'extract.pl' )
                              : $grabSource =~
                              m#^([a-zA-Z0-9._-]+):([ /a-zA-Z0-9._+-]+)$#s
                              ? ( $1, $2 )
                              : die "Cannot grab from $grabSource";
                            $cellarDir .= '/Y_Cellar '
                              . POSIX::strftime( '%Y-%m-%d %H-%M-%S%z',
                                localtime )
                              . ' '
                              . $host;
                            mkdir $cellarDir;
                            chdir $cellarDir;
                            open my $fh,
                              qq^| ssh $host 'perl "$extract" -tar -^
                              . q^ 2>/dev/null' | tar -x -f -^;
                            binmode $fh;
                            print {$fh}
                              FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker(
                              )->encode($toGrab);
                        }
                        require FileMgt106::Folders::FolderClean;
                        $hints->beginInteractive(1);
                        FileMgt106::Folders::FolderClean::deepClean('.');
                        $hints->commit;
                        $cellarScanner =
                          FileMgt106::Scanning::ScanMaster->new( $hints,
                            decode_utf8( getcwd() ),
                            $self->fileSystemObj );
                        $cellarScanner->dequeued;
                    }
                    while ( my ( $dir, $scalar ) = each %$missing ) {
                        $hints->beginInteractive;
                        eval {
                            $scalar = (
                                $scanners{$dir}
                                  || FileMgt106::Scanning::Scanner->new(
                                    $dir, $hints,
                                    $self->fileSystemObj->statFromGid(
                                        ( stat $dir )[STAT_GID]
                                    )
                                  )
                            )->infill($scalar);
                        };
                        warn "infill $dir: $@" if $@;
                        $hints->commit;
                        if ($scalar) {
                            $missing->{$dir} = $scalar;
                        }
                        else {
                            delete $missing->{$dir};
                        }
                    }
                    if ( defined $cellarDir ) {
                        require FileMgt106::Folders::FolderClean;
                        $hints->beginInteractive(1);
                        FileMgt106::Folders::FolderClean::deepClean($cellarDir);
                        $hints->commit;
                        $cellarScanner->dequeued;
                        push @rmdirList, $cellarDir;
                    }
                    last unless %$missing;
                }
                $missing = _filterByFileName($missing)
                  unless grep { /:\+$/s; } @grabSources;
                while ( my ( $path, $missing ) = each %$missing ) {
                    my $tmpFile = catfile( $path, "\N{U+26A0}$$.txt" );
                    open my $fh, '>', $tmpFile;
                    binmode $fh;
                    print {$fh}
                      FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker(
                      )->encode($missing);
                    close $fh;
                    rename $tmpFile, catfile( $path, '⚠️.txt' );
                }
                rmdir $_ foreach @rmdirList;
            }
            else {    # no grab sources
                binmode STDOUT;
                print
                  FileMgt106::Catalogues::LoadSaveNormalize::jsonMachineMaker()
                  ->encode($missing);
            }
        }
        $hints->disconnect if $hints;
        if (@toRestamp) {
            require FileMgt106::Folders::FolderOrganise;
            FileMgt106::Folders::FolderOrganise::restampFolder($_)
              foreach @toRestamp;
        }
    };

    my $legacyArgumentsAcceptor = sub {

        my ( %locs, $repolocOptions, @scanMasterCliConfigClosures );
        my $startFolder = $self->startFolder;

        foreach (@_) {

            next if !defined $_ || $_ eq '';

            local $_ = decode_utf8 $_;

            if (/^-+sync=(.+)$/) {
                chdir $startFolder;
                chdir $1 or next;
                $syncDestination = decode_utf8 getcwd();
                next;
            }
            elsif (/^-+((?:clean|flat|datemark|dayfolder|restamp).*)$/) {
                $cleaningFlag = $1;
                next;
            }
            elsif (/^-+autonumber/) {
                require FileMgt106::Folders::FolderOrganise;
                push @scanMasterCliConfigClosures, sub {
                    my ( $scanMaster, $path ) = @_;
                    $scanMaster->addScalarTaker(
                        sub {
                            my ( $scalar, $blobref, $runner ) = @_;
                            FileMgt106::Folders::FolderOrganise::automaticNumbering(
                                $path, $scalar );
                        }
                    );
                };
                next;
            }
            elsif (/^-+(filter|split|explode).*$/) {
                die "scan.pl does not support -$1 any more; use extract.pl";
            }
            elsif (/^-+grab=?(.*)/) {
                push @grabSources, $1;
                next;
            }
            elsif (/^-+noaction/) {
                push @scanMasterCliConfigClosures, sub {
                    $_[0]->prohibitActions;
                };
                next;
            }
            elsif (/^-+read-?only/) {
                push @scanMasterCliConfigClosures, sub {
                    $_[0]->setFrotl(604_800);
                };
                next;
            }
            elsif (/^-+cat/) {
                push @scanMasterCliConfigClosures, sub {
                    my ( $scanMaster, $path ) = @_;
                    $scanMaster->addScalarTaker(
                        sub {
                            binmode STDOUT;
                            print ${ $_[1] };
                        }
                    );
                };
                next;
            }
            elsif (/^-+repo=?(.*)/) {
                local $_ = $1;
                my $loc =
                  !$_ ? $startFolder : m#^/#s ? $_ : catdir( $startFolder, $_ );
                push @scanMasterCliConfigClosures,
                  sub { $_[0]->addJbzFolder($loc); };
                next;
            }
            elsif (/^-+aperture/) {
                push @scanMasterCliConfigClosures, sub {
                    my ( $scanMaster, $path ) = @_;
                    if ( $path =~ /\.aplibrary$/s ) {
                        require FileMgt106::Scanning::ScanMasterAperture;
                        warn "Using Aperture scan master for $path";
                        bless $scanMaster,
                          'FileMgt106::Scanning::ScanMasterAperture';
                    }
                };
                next;
            }
            elsif (/^-+stash=(.+)/) {
                local $_ = $1;
                $locs{stash} = m#^/#s ? $_ : catdir( $startFolder, $_ );
                next;
            }
            elsif (/^-+backup=?(.*)/) {
                local $_ = $1;
                $locs{repo} =
                  !$_ ? $startFolder : m#^/#s ? $_ : catdir( $startFolder, $_ );
                next;
            }
            elsif (/^-+git=?(.*)/) {
                local $_ = $1;
                $locs{git} =
                  !$_ ? $startFolder : m#^/#s ? $_ : catdir( $startFolder, $_ );
                next;
            }
            elsif (/^-+resolve/) {
                $locs{resolve} = 1;
                next;
            }

            $hints ||= $self->hintsObj;

            if (/^-+migrate(?:=(.+))?/) {
                $self->migrate( undef, $1 );
                next;
            }

            $_ = catdir( $startFolder, $_ ) unless m#^/#s;
            my @argumentStat = lstat;
            if ( -l _ ) {
                if ( readlink =~ /([0-9a-zA-Z]{40})/ ) {
                    $missing->{$_} = $1;
                    next;
                }
                @argumentStat = stat;
            }

            my ( $target, $root, $ext );
            if ( -f _
                and ( $root, $ext ) =
                /(.*)(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$/si )
            {
                $target =
                  FileMgt106::Catalogues::LoadSaveNormalize::loadNormalisedScalar(
                    $root . $ext );
                $target =
                  FileMgt106::Catalogues::LoadSaveNormalize::parseText(
                    $root . $ext )
                  if !$target && $ext =~ /txt|yml/i;
            }
            elsif ( -d _ && @grabSources && chdir $_ ) {
                $root = decode_utf8 getcwd();
                $ext  = '';
                $hints->beginInteractive;
                $target =
                  FileMgt106::Scanning::Scanner->new( $root, $hints )->scan;
                $hints->commit;
            }

            if ($target) {
                $scalarAcceptor->(
                    $target, $root, $ext, \@argumentStat, \%locs
                );
            }
            elsif ( -d _ && chdir $_ ) {
                $folderAcceptor->(
                    %locs
                    ? sub { $_[0]->setRepoloc( \%locs, $repolocOptions ); }
                    : (),
                    @scanMasterCliConfigClosures,
                );
            }
            else {
                warn "Ignored: $_";
            }

        }

    };

    $scalarAcceptor, $folderAcceptor, $finisher, $legacyArgumentsAcceptor;

}

sub _filterByFileName {
    my ($src) = @_;
    my %filtered;
    foreach (
        grep { !/(?:^~WRL[0-9]+\.tmp|\.dta|\.[zZ][iI][pP])$/s; }
        keys %$src
      )
    {
        my $v = $src->{$_};
        if ( 'HASH' eq ref $v ) {
            $v = _filterByFileName($v);
            next unless %$v;
        }
        $filtered{$_} = $v;
    }
    \%filtered;
}

1;
