package FileMgt106::CLI::ScanCLI;

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
use utf8;

require POSIX;
use Cwd qw(getcwd);
use Encode qw(decode_utf8);
use File::Basename qw(dirname basename);
use File::Spec::Functions qw(catdir catfile rel2abs);
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_GID STAT_MTIME);
use FileMgt106::LoadSaveNormalize;
use FileMgt106::ScanMaster;
use FileMgt106::Scanner;

sub makeProcessor {

    my ( $self, @grabSources ) = @_;
    my ( $startFolder, $perl5dir, $fs ) = @$self;
    my ( $hints, $missing, %scanners, $cleaningFlag,
        $syncDestination, @toRestamp );

    my $scalarAcceptor = sub {
        my ( $scalar, $path, $fileExtension, $targetStatRef, $options ) = @_;
        push @toRestamp, $path if $options->{restamp};
        $hints ||=
          FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) );
        delete $scalar->{$_} foreach grep { /\//; } keys %$scalar;
        my $dir = $path;
        my ($grabExclusions);
        if ( $dir =~ m#(.+)/[⚠✘=+-][^/]*$#s ) {
            $path = $1;
            my $rgid = ( stat _ )[STAT_GID];
            chdir $1 or die "chdir $1: $!";
            $dir = decode_utf8 getcwd();
            warn "Infilling $dir with rgid=$rgid and $fileExtension file";
            $hints->beginInteractive;
            eval {
                $scalar = (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints,
                        $fs->statFromGid( ( stat $dir )[STAT_GID] )
                    )
                )->infill($scalar);
            };
            warn "infill $dir: $@" if $@;
            $hints->commit;
        }
        else {
            my $rgid = $targetStatRef->[STAT_GID];
            mkdir $dir unless -e $dir;
            chown 0, $rgid, $dir;
            chmod 0770, $dir;
            chdir $dir or die "chdir $dir: $!";
            $dir = decode_utf8 getcwd();
            my $message = $fileExtension ? " and $fileExtension file" : '';
            if ( my ($buildExclusionsFile) = grep { -f $_; } '🚫.txt' ) {
                $message .= ', with build exclusions';
                $scalar->{$buildExclusionsFile} = [];
                ( $scalar, my $excluded ) = _filterExclusions(
                    $scalar,
                    FileMgt106::LoadSaveNormalize::loadNormalisedScalar(
                        $buildExclusionsFile)
                );
                if ($excluded) {
                    my $tmpFile = "✘$$.txt";
                    open my $fh, '>', $tmpFile;
                    binmode $fh;
                    print {$fh}
                      FileMgt106::LoadSaveNormalize::jsonMachineMaker()
                      ->encode($excluded);
                    close $fh;
                    rename $tmpFile, '✘📁.txt';
                    $scalar->{'✘📁.txt'} = [];
                }
                else {
                    unlink '✘📁.txt';
                }
            }
            if ( my ($grabExclusionsFile) = grep { -f $_; } '⛔️.txt',
                "\N{U+26A0}.txt" )
            {
                $scalar->{$grabExclusionsFile} = []
                  unless $grabExclusionsFile eq "\N{U+26A0}.txt";
                if (@grabSources) {
                    $message .= ', with grab exclusions';
                    $grabExclusions =
                      FileMgt106::LoadSaveNormalize::loadNormalisedScalar(
                        $grabExclusionsFile);
                }
            }
            warn "Rebuilding $dir with rgid=$rgid$message\n";
            $hints->beginInteractive;
            eval {
                (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints, $fs->statFromGid($rgid)
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
        }
        if ( ref $scalar && keys %$scalar ) {
            if (@grabSources) {
                my ( $toGrab, $excluded ) =
                  _filterExclusions( $scalar, $grabExclusions );
                if ($excluded) {
                    $toGrab->{'.caseid'} = delete $excluded->{'.caseid'}
                      if defined $excluded->{'.caseid'};
                    my $tmpFile = "✘$$.txt";
                    open my $fh, '>', $tmpFile;
                    binmode $fh;
                    print {$fh}
                      FileMgt106::LoadSaveNormalize::jsonMachineMaker()
                      ->encode($excluded);
                    close $fh;
                    rename $tmpFile, '✘⬇️.txt';
                }
                else {
                    unlink '✘⬇️.txt';
                }
                $missing->{$dir} = $toGrab if $toGrab;
            }
            else {
                _saveMissingFilesCatalogues( $path, $scalar );
            }
        }
    };

    my $folderAcceptor = sub {
        my (@scanMasterCliConfigClosures) = @_;
        $hints ||=
          FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) );
        my $dir = decode_utf8 getcwd();
        if ($syncDestination) {
            my $destination = catdir( $syncDestination, basename($dir) );
            mkdir $destination;
            my ( @extrasSource, @extrasDestination );
            my ($s) =
              FileMgt106::Scanner->new( $dir, $hints, @extrasSource )->scan;
            FileMgt106::Scanner->new( $destination, $hints, @extrasDestination )
              ->scan( 0, $s );
            $hints->commit;
            return;
        }
        if ($cleaningFlag) {
            if ( $cleaningFlag =~ /dayfolder/i ) {
                warn "One folder per day for files in $dir";
                require FileMgt106::FolderOrganise;
                FileMgt106::FolderOrganise::categoriseByDay($dir);
            }
            if ( $cleaningFlag =~ /datemark/i ) {
                warn "Datemarking $dir";
                require FileMgt106::FolderOrganise;
                FileMgt106::FolderOrganise::datemarkFolder($dir);
            }
            if ( $cleaningFlag =~ /restamp/i ) {
                warn "Re-timestamping $dir";
                require FileMgt106::FolderOrganise;
                FileMgt106::FolderOrganise::restampFolder($dir);
            }
            if ( $cleaningFlag =~ /flat/i ) {
                warn "Flattening $dir";
                require FileMgt106::FolderOrganise;
                $hints->beginInteractive(1);
                FileMgt106::FolderOrganise::flattenCwd();
                $hints->commit;
            }
            if ( $cleaningFlag =~ /rename/i ) {
                warn "Renaming in $dir";
                $hints->beginInteractive(1);
                FileMgt106::LoadSaveNormalize::renameFilesToNormalisedScannable(
                    '.');
                $hints->commit;
            }
            elsif ( $cleaningFlag =~ /clean/i ) {
                warn "Deep cleaning $dir";
                require FileMgt106::FolderClean;
                $hints->beginInteractive(1);
                FileMgt106::FolderClean::deepClean('.');
                $hints->commit;
            }
            return if $cleaningFlag =~ /only/i;
        }
        my $scanMaster = FileMgt106::ScanMaster->new( $hints, $dir, $fs );
        $_->( $scanMaster, $dir ) foreach @scanMasterCliConfigClosures;
        $scanMaster->dequeued;
    };

    my $finisher = sub {
        if ($missing) {
            my @rmdirList;
          SOURCE: foreach (@grabSources) {
                my $grabSource = $_;    # a true copy, not a loop alias variable
                unless ($grabSource) {
                    binmode STDOUT;
                    print FileMgt106::LoadSaveNormalize::jsonMachineMaker()
                      ->encode($missing);
                    next;
                }
                my ( $cellarScanner, $cellarDir );
                unless ( $grabSource eq 'done' ) {
                    $cellarDir = dirname($perl5dir);
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
                          : die $grabSource;
                        $cellarDir .=
                            '/Y_Cellar '
                          . POSIX::strftime( '%Y-%m-%d %H-%M-%S%z', localtime )
                          . ' '
                          . $host;
                        mkdir $cellarDir;
                        chdir $cellarDir;
                        open my $fh,
                          qq^| ssh $host 'perl "$extract" -tar -'^
                          . ' | tar -x -f -';
                        binmode $fh;
                        print {$fh}
                          FileMgt106::LoadSaveNormalize::jsonMachineMaker()
                          ->encode($toGrab);
                    }
                    require FileMgt106::FolderClean;
                    $hints->beginInteractive(1);
                    FileMgt106::FolderClean::deepClean('.');    #use Scanner
                    $hints->commit;
                    $cellarScanner =
                      FileMgt106::ScanMaster->new( $hints,
                        decode_utf8( getcwd() ), $fs );
                    $cellarScanner->dequeued;
                }
                while ( my ( $dir, $scalar ) = each %$missing ) {
                    $hints->beginInteractive;
                    eval {
                        $scalar = (
                            $scanners{$dir} || FileMgt106::Scanner->new(
                                $dir, $hints,
                                $fs->statFromGid( ( stat $dir )[STAT_GID] )
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
                    require FileMgt106::FolderClean;
                    $hints->beginInteractive(1);
                    FileMgt106::FolderClean::deepClean($cellarDir);
                    $hints->commit;
                    $cellarScanner->dequeued;
                    push @rmdirList, $cellarDir;
                }
                last unless %$missing;
            }
            $missing = _filterByFileName($missing)
              unless grep { /:\+$/s; } @grabSources;
            _saveMissingFilesCatalogues(%$missing);
            rmdir $_ foreach @rmdirList;
        }
        $hints->disconnect if $hints;
        require FileMgt106::FolderOrganise;
        FileMgt106::FolderOrganise::restampFolder($_) foreach @toRestamp;
    };

    my $legacyArgumentsAcceptor = sub {

        my ( %locs, $repolocOptions, @scanMasterCliConfigClosures );

        foreach (@_) {

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
                require FileMgt106::FolderOrganise;
                push @scanMasterCliConfigClosures, sub {
                    my ( $scanMaster, $path ) = @_;
                    $scanMaster->addScalarTaker(
                        sub {
                            my ( $scalar, $blobref, $runner ) = @_;
                            FileMgt106::FolderOrganise::automaticNumbering(
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
                push @grabSources, $1 || '';
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
                    $_[0]->setFrotl(
                        2_000_000_000    # This will go wrong in 2033
                    );
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
                my $loc = !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                push @scanMasterCliConfigClosures,
                  sub { $_[0]->addJbzFolder($loc); };
                next;
            }
            elsif (/^-+aperture/) {
                push @scanMasterCliConfigClosures, sub {
                    my ( $scanMaster, $path ) = @_;
                    if ( $path =~ /\.aplibrary$/s ) {
                        require FileMgt106::ScanMasterAperture;
                        warn "Using Aperture scan master for $path";
                        bless $scanMaster, 'FileMgt106::ScanMasterAperture';
                    }
                };
                next;
            }
            elsif (/^-+stash=(.+)/) {
                local $_ = $1;
                $locs{stash} = m#^/#s ? $_ : "$startFolder/$_";
                next;
            }
            elsif (/^-+backup=?(.*)/) {
                local $_ = $1;
                $locs{repo} =
                  !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                next;
            }
            elsif (/^-+git=?(.*)/) {
                local $_ = $1;
                $locs{git} =
                  !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                next;
            }

            $hints ||=
              FileMgt106::Database->new(
                catfile( dirname($perl5dir), '~$hints' ) );

            if (/^-+migrate(?:=(.+))?/) {
                $self->migrate( undef, $1 );
                next;
            }

            $_ = "$startFolder/$_" unless m#^/#s;
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
                  FileMgt106::LoadSaveNormalize::loadNormalisedScalar(
                    $root . $ext );
                $target =
                  FileMgt106::LoadSaveNormalize::parseText( $root . $ext )
                  if !$target && $ext =~ /txt|yml/i;
            }
            elsif ( -d _ && @grabSources && chdir $_ ) {
                $root = decode_utf8 getcwd();
                $ext  = '';
                $hints->beginInteractive;
                $target = FileMgt106::Scanner->new( $root, $hints )->scan;
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

    my $chooserMaker = sub {
        my ($initialise) = @_;
        sub {
            my ( $catalogue, $canonical, $fileExtension, $devNo ) = @_;
            my $target =
              FileMgt106::LoadSaveNormalize::loadNormalisedScalar($catalogue);
            unlink $canonical if -l $canonical;
            unlink $canonical . $fileExtension
              if -l $canonical . $fileExtension;
            return $target, $canonical if -d $canonical;
            if (
                ( my $sha1hex = $target->{'.caseid'} )
                && (
                    $hints ||= FileMgt106::Database->new(
                        catfile( dirname($perl5dir), '~$hints' )
                    )
                )
              )
            {
                $hints->beginInteractive;
                my $iterator =
                  $hints->{searchSha1}->( pack( 'H*', $sha1hex ), $devNo );
                my $destination;
                while ( my ( $path, $statref, $locid ) = $iterator->() ) {
                    next if defined $destination;
                    next if $path =~ m#/Y_Cellar.*/#;
                    next
                      unless $path =~
                      s#(/\@[^/]+| \(mirrored from [^/]+\))/.*\.caseid$#$1#s;
                    $destination = $path;
                }
                $hints->commit;
                if ( defined $destination ) {
                    symlink $destination, $canonical;
                    return $target, $destination;
                }
            }
            if ( $initialise && mkdir $canonical ) {
                {
                    open my $fh, '>', catfile( $canonical, '🚫.txt' );
                    print {$fh} '{}';
                }
                {
                    open my $fh, '>', catfile( $canonical, '⛔️.txt' );
                    print {$fh} '{".":"no"}';
                }
                return $target, $canonical;
            }
            else {
                symlink rel2abs($catalogue), $canonical . $fileExtension;
                return;
            }
        };
    };

    $scalarAcceptor, $folderAcceptor, $finisher, $legacyArgumentsAcceptor,
      $chooserMaker;

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

sub _saveMissingFilesCatalogues {
    while ( my ( $path, $missing ) = splice @_, 0, 2 ) {
        my $tmpFile = catfile( $path, "\N{U+26A0}$$.txt" );
        open my $fh, '>', $tmpFile;
        binmode $fh;
        print {$fh}
          FileMgt106::LoadSaveNormalize::jsonMachineMaker()->encode($missing);
        close $fh;
        rename $tmpFile, catfile( $path, "\N{U+26A0}.txt" );
    }
}

sub _filterExclusions {
    my ( $src, $excl ) = @_;
    return unless defined $src;
    return $src unless $excl;
    return wantarray ? ( undef, $src ) : undef if !ref $excl || $excl->{'.'};
    my %included = %$src;
    my %excluded;
    ( $included{$_}, $excluded{$_} ) =
      _filterExclusions( $included{$_}, $excl->{$_} )
      foreach keys %$excl;
    delete $included{$_}
      foreach grep { !defined $included{$_}; } keys %included;
    my $included = %included ? \%included : undef;
    return $included unless wantarray;
    delete $excluded{$_}
      foreach grep { !defined $excluded{$_}; } keys %excluded;
    $included, %excluded ? \%excluded : undef;
}

1;
