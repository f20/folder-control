package FileMgt106::CLI::ScanCLI;

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

require POSIX;
use Cwd qw(getcwd);
use Encode qw(decode_utf8);
use File::Basename qw(dirname basename);
use File::Spec::Functions qw(catdir catfile rel2abs);
use JSON;
use FileMgt106::Database;
use FileMgt106::FileSystem;
use FileMgt106::LoadSave;
use FileMgt106::ScanMaster;
use FileMgt106::Scanner;

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub process {
    my ( $self, @arguments ) = @_;
    local $_ = $arguments[0];
    return $self->help unless defined $_;
    return $self->$_(@arguments) if s/^-+//s && UNIVERSAL::can( $self, $_ );
    my ( $processScalar, $processCwd, $finish, $processLegacyArguments ) =
      $self->makeProcessor;
    $processLegacyArguments->(@arguments);
    $finish->();
}

sub help {
    warn <<EOW;
Usage:
    scan.pl -help
    scan.pl -migrate <old-hints-file>
    scan.pl <legacy-arguments>
EOW
}

sub autograb {
    my ( $self,        @arguments ) = @_;
    my ( $startFolder, $perl5dir )  = @$self;
    my ( $processScalar, $processCwd, $finish, undef, $chooserMaker ) =
      $self->makeProcessor( map { /^-+grab=(.+)/s ? $1 : (); } @arguments );
    my $chooser =
      $chooserMaker->( map { /^-+caseid=([0-9-]+)/ ? $1 : (); } @arguments );
    my @fileList = map {
        /^-$/s
          ? eval {
            local $/ = "\n";
            map { chomp; $_; } <STDIN>;
          }
          : $_;
    } @arguments;

    foreach (@fileList) {
        chdir $startFolder;
        my @targetStat = stat;
        -f _ or next;
        my @components = split /\/+/;
        my $canonical  = pop @components;
        next
          unless $canonical =~ s/(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$//s;
        $canonical .= " (mirrored from $components[1])";
        if ( my ( $scalar, $folder ) = $chooser->( $_, $canonical, $1 ) ) {
            $processScalar->( $scalar, $folder, $1, \@targetStat, 1 );
        }
    }
    $finish->();
}

sub migrate {
    my ( $self, $command, $oldFileName ) = @_;
    my ( $startFolder, $perl5dir ) = @$self;
    $oldFileName = rel2abs( $oldFileName, $startFolder )
      if defined $oldFileName;
    chdir dirname($perl5dir) or die "chdir dirname($perl5dir): $!";
    unless ( $oldFileName && -f $oldFileName ) {
        my $mtime = ( stat '~$hints' )[STAT_MTIME]
          or die 'No existing hints file';
        $mtime = POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z', localtime($mtime) );
        $oldFileName = '~$hints ' . $mtime;
        rename '~$hints', $oldFileName
          or die "Cannot move ~\$hints to $oldFileName: $!";
    }
    my $hints = FileMgt106::Database->new('~$hints')
      or die "Cannot create new database";
    my $db = $hints->{dbHandle};
    $db->{AutoCommit} = 1;
    $hints->{dbHandle}
      ->do( 'pragma journal_mode=' . ( /nowal/i ? 'delete' : 'wal' ) )
      if /wal/i;
    $db->do("attach '$oldFileName' as old");

    if ( $db->do('begin exclusive transaction') ) {
        $db->do('delete from main.locations');
        $db->do('create temporary table t0'
              . ' (nl integer primary key, ol integer, nr integer)' );
        $db->do('create temporary table t1'
              . ' (nl integer primary key, ol integer, nr integer)' );
        $db->do("insert or replace into t0 (nl, ol) values (0, 0)");
        my $total = 0;

        my $prettifyWarning = sub {
            my ( $number, $spaces ) = @_;
            do { } while $number =~ s/([0-9])([0-9]{3})(?:,|$)/$1,$2/s;
            $spaces -= length $number;
            ( $spaces > 0 ? ' ' x $spaces : '' ) . $number;
        };

        warn $prettifyWarning->( 'Level', 5 ),
          $prettifyWarning->( 'Added', 15 ),
          $prettifyWarning->( 'Total', 15 ), "\n";
        foreach ( 0 .. 999 ) {
            last
              if $db->do( 'insert into t1 (ol, nr) select locid, nr'
                  . ' from old.locations as loc'
                  . ' inner join t0 on (parid = ol)' ) < 1;
            $db->do('update t1 set nr=nl where nr is null')
              unless $_;
            my $added =
              $db->do( 'insert or ignore into main.locations'
                  . ' (locid, parid, name, rootid, ino, size, mtime, sha1)'
                  . ' select t1.nl, t0.nl, loc.name, t1.nr, loc.ino, loc.size, loc.mtime, loc.sha1'
                  . ' from old.locations as loc, t0, t1 where locid=t1.ol and parid=t0.ol'
              );
            $total += $added;
            warn $prettifyWarning->( $_, 5 ),
              $prettifyWarning->( $added, 15 ),
              $prettifyWarning->( $total, 15 ), "\n";
            $db->do('delete from t0');
            $db->do('insert into t0 select * from t1');
            $db->do('delete from t1');
            $db->do('insert into t1 (nl) select max(nl) from t0');
        }
        $db->do('drop table t0');
        $db->do('drop table t1');
        warn 'Committing changes';
        my $status;
        sleep 2 while !( $status = $db->commit );
    }
    else {
        warn 'New database is in use: no migration done';
    }
}

sub makeProcessor {

    my ( $self,        @grabSources ) = @_;
    my ( $startFolder, $perl5dir )    = @$self;
    my (
        $hints,      $filter,          @baseScalars,
        $missing,    %scanners,        $cleaningFlag,
        $filterFlag, $syncDestination, @toRestamp
    );

    my $processScalar = sub {
        my ( $scalar, $path, $fileExtension, $targetStatRef, $restampFlag ) =
          @_;
        push @toRestamp, $path if $restampFlag;
        $hints ||=
          FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) );
        if ( $filterFlag && $filterFlag =~ /split/ ) {
            while ( my ( $k, $v ) = each %$scalar ) {
                local $_ = $k;
                s#/#..#g;
                FileMgt106::LoadSave::saveJbzPretty( "$path \$$_.jbz",
                    ref $v ? $v : { $k => $v } );
            }
            return;
        }
        elsif ( $filterFlag && $filterFlag =~ /explode/ ) {
            my ($module) =
              grep { s#^/(FilterFactory::)#FileMgt106::$1#; } keys %$scalar;
            my $exploded =
                $module && eval "require $module"
              ? $module->new($scalar)->exploded
              : (
                require FileMgt106::FilterFactory::ByType,
                FileMgt106::FilterFactory::ByType::explodeByType($scalar)
              );
            $path =~ s/\.aplibrary$//s;
            while ( my ( $k, $v ) = each %$exploded ) {
                FileMgt106::LoadSave::saveJbzPretty( "$path \$$k.jbz", $v )
                  if ref $v;
            }
            return;
        }
        delete $scalar->{$_} foreach grep { /\//; } keys %$scalar;
        my $dir = $path;
        if ($filterFlag) {
            unless ($filter) {
                require FileMgt106::CLI::Miscellaneous;
                $filter =
                  FileMgt106::CLI::Miscellaneous::makeHintsFilterQuick( $hints,
                    $filterFlag );
                $filter->($_) foreach @baseScalars;
            }
            warn "Filtering '$path$fileExtension' with $filterFlag";
            $scalar = $filter->($scalar);
            undef $filter if $filterFlag =~ /separate/i;
        }
        elsif ( $dir =~ /(.+)\+missing$/s && -d $1 ) {
            my $rgid = ( stat _ )[STAT_GID];
            chdir $1 or die "chdir $1: $!";
            $dir = decode_utf8 getcwd();
            warn "Infilling $dir with rgid=$rgid and $fileExtension file";
            $hints->beginInteractive;
            eval {
                $scalar = (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints,
                        $hints->statFromGid( ( stat $dir )[STAT_GID] )
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
            warn "Rebuilding $dir with rgid=$rgid"
              . ( $fileExtension ? " and $fileExtension file" : '' );
            $hints->beginInteractive;
            eval {
                (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints, $hints->statFromGid($rgid)
                    )
                )->scan( 0, $scalar );
            };
            warn "scan $dir: $@" if $@;
            $hints->commit;
            utime time, $targetStatRef->[STAT_MTIME], $dir;
        }
        my $missingFile = "$path+missing.jbz";
        $missingFile =~ s/\+missing\+missing/+missing/;
        unlink $missingFile;
        if ( ref $scalar && keys %$scalar ) {
            if (@grabSources) {
                $missing->{$dir} = $scalar;
            }
            else {
                FileMgt106::LoadSave::saveJbz( $missingFile, $scalar );
            }
        }
    };

    my $processCwd = sub {
        my (@scanMasterConfigClosures) = @_;
        $hints ||=
          FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) );
        my $dir = decode_utf8 getcwd();
        if ($syncDestination) {
            my $destination = catdir( $syncDestination, basename($dir) );
            mkdir $destination;
            my ( @extrasSource, @extrasDestination );
            push @extrasDestination, FileMgt106::FileSystem::noInodeStat()
              if $destination =~ m#^/Volumes/#;
            push @extrasSource, FileMgt106::FileSystem::noInodeStat()
              if $dir =~ m#^/Volumes/#;
            my ($s) =
              FileMgt106::Scanner->new( $dir, $hints, @extrasSource )->scan;
            require FileMgt106::CLI::Miscellaneous;
            FileMgt106::Scanner->new( $destination, $hints, @extrasDestination )
              ->scan( 0, FileMgt106::CLI::Miscellaneous::simpleDedup($s) );
            $hints->commit;
            return;
        }
        if ($cleaningFlag) {
            if ( $cleaningFlag =~ /dayfolder/i ) {
                warn "One folder per day for files in $dir";
                require FileMgt106::FolderTidy;
                FileMgt106::FolderTidy::categoriseByDay($dir);
            }
            if ( $cleaningFlag =~ /datemark/i ) {
                warn "Datemarking $dir";
                require FileMgt106::FolderTidy;
                FileMgt106::FolderTidy::datemarkFolder($dir);
            }
            if ( $cleaningFlag =~ /restamp/i ) {
                warn "Re-timestamping $dir";
                require FileMgt106::FolderTidy;
                FileMgt106::FolderTidy::restampFolder($dir);
            }
            if ( $cleaningFlag =~ /flat/i ) {
                warn "Flattening $dir";
                require FileMgt106::FolderTidy;
                $hints->beginInteractive(1);
                FileMgt106::FolderTidy::flattenCwd();
                $hints->commit;
            }
            if ( $cleaningFlag =~ /rename/i ) {
                warn "Renaming in $dir";
                $hints->beginInteractive(1);
                FileMgt106::LoadSave::normaliseFileNames('.');
                $hints->commit;
            }
            elsif ( $cleaningFlag =~ /clean/i ) {
                warn "Deep cleaning $dir";
                require FileMgt106::FolderTidy;
                $hints->beginInteractive(1);
                FileMgt106::FolderTidy::deepClean('.');
                $hints->commit;
            }
            return if $cleaningFlag =~ /only/i;
        }
        my $scanner = FileMgt106::ScanMaster->new( $hints, $dir );
        $_->( $scanner, $dir ) foreach @scanMasterConfigClosures;
        $scanner->dequeued;
    };

    my $finish = sub {
        if ($missing) {
            my @rmdirList;
            foreach my $grabSource (@grabSources) {
                unless ($grabSource) {
                    my $missingFile = "$startFolder/+missing.jbz";
                    FileMgt106::LoadSave::saveJbz( $missingFile, $missing );
                    die "Do your own grab: $missingFile";
                }
                my ( $cellarScanner, $cellarDir );
                unless ( $grabSource eq 'done' ) {
                    $cellarDir = dirname($perl5dir);
                    if ( -d ( my $d = $cellarDir . '/~$grab' ) ) {
                        $cellarDir = $d;
                    }
                    {
                        my ( $host, $extract ) =
                          $grabSource =~ /^([a-zA-Z0-9._-]+)$/s
                          ? ( $1, 'extract.pl' )
                          : $grabSource =~
                          m#^([a-zA-Z0-9._-]+):([ /a-zA-Z0-9._-]+)$#s
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
                        print {$fh} encode_json($missing);
                    }
                    require FileMgt106::FolderTidy;
                    $hints->beginInteractive(1);
                    FileMgt106::FolderTidy::deepClean('.');
                    $hints->commit;
                    $cellarScanner =
                      FileMgt106::ScanMaster->new( $hints,
                        decode_utf8 getcwd() );
                    $cellarScanner->dequeued;
                }
                while ( my ( $dir, $scalar ) = each %$missing ) {
                    $hints->beginInteractive;
                    eval {
                        $scalar = (
                            $scanners{$dir} || FileMgt106::Scanner->new(
                                $dir,
                                $hints,
                                $hints->statFromGid( ( stat $dir )[STAT_GID] )
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
                    require FileMgt106::FolderTidy;
                    $hints->beginInteractive(1);
                    FileMgt106::FolderTidy::deepClean($cellarDir);
                    $hints->commit;
                    $cellarScanner->dequeued;
                    push @rmdirList, $cellarDir;
                }
                last unless %$missing;
            }
            while ( my ( $dir, $stillMissing ) = each %$missing ) {
                FileMgt106::LoadSave::saveJbzPretty(
                    catfile( $dir, "\N{U+26A0}.jbz" ),
                    $stillMissing )
                  if $stillMissing;
            }
            rmdir $_ foreach @rmdirList;
        }
        $hints->disconnect if $hints;
        require FileMgt106::FolderTidy;
        FileMgt106::FolderTidy::restampFolder($_) foreach @toRestamp;
    };

    my $processLegacyArguments = sub {

        my @scanMasterConfigClosures;

        foreach (@_) {

            local $_ = decode_utf8 $_;

            if (/^-+watch=?(.*)/) {
                my $module   = 'Daemon112::SimpleWatch';
                my $nickname = 'watch';
                my $logging  = $1;
                my ( $hintsFile, $top, $repo, $git, $jbz, $parent ) =
                  map { /^-+watch/ ? () : /^-/ ? undef : $_; } @_;
                $_ = rel2abs($_)
                  foreach grep { defined $_; } $hintsFile, $top, $repo,
                  $git,
                  $jbz,
                  $parent;
                $parent ||= $startFolder;
                require Daemon112::Daemon;
                Daemon112::Daemon->run(
                    $module, $nickname, $logging, $hintsFile, $top,
                    $repo,   $git,      $jbz,     $parent
                );
            }
            elsif (/^-+sync=(.+)$/) {
                chdir $startFolder;
                chdir $1 or next;
                $syncDestination = decode_utf8 getcwd();
                next;
            }
            elsif (/^-+((?:clean|flat|datemark|dayfolder|restamp).*)$/) {
                $cleaningFlag = $1;
                next;
            }
            elsif (/^-+((?:filter).*)$/) {
                $filterFlag = $1;
                next;
            }
            elsif (/^-+((?:split|explode).*)$/) {
                $filterFlag = $1 . 'nodb';
                next;
            }
            elsif (/^-+base=(.*)/) {
                $filterFlag .= 'nodb';
                @baseScalars =
                  map { FileMgt106::LoadSave::loadNormalisedScalar($_); }
                  split /:/,
                  $1;
                next;
            }
            elsif (/^-+grab=?(.*)/) {
                push @grabSources, $1 || '';
                next;
            }
            elsif (/^-+read-?only/) {
                push @scanMasterConfigClosures, sub {
                    $_[0]->setFrotl(
                        2_000_000_000    # This will go wrong in 2033
                    );
                };
                next;
            }
            elsif (/^-+cat/) {
                push @scanMasterConfigClosures, sub {
                    $_[0]->addScalarTaker(
                        sub {
                            binmode STDOUT;
                            print ${ $_[1] };
                        }
                    );
                };
                next;
            }
            elsif (/^-+(?:jbz|repo)=?(.*)/) {
                local $_ = $1;
                my $loc = !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                push @scanMasterConfigClosures,
                  sub { $_[0]->addJbzFolder($loc); };
                next;
            }
            elsif (/^-+backup=?(.*)/) {
                local $_ = $1;
                my $loc = !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                push @scanMasterConfigClosures,
                  sub { $_[0]->setRepoloc( { repo => $loc } ); };
                next;
            }
            elsif (/^-+git=?(.*)/) {
                local $_ = $1;
                my $loc = !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
                push @scanMasterConfigClosures,
                  sub { $_[0]->setRepoloc( { git => $loc } ); };
                next;
            }
            $hints ||=
              FileMgt106::Database->new(
                catfile( dirname($perl5dir), '~$hints' ) )
              unless $filterFlag && $filterFlag =~ /nodb/i;
            if (/^-+aperture=?(.*)/) {
                my $jbzDir = $startFolder;
                if ($1) {
                    chdir $startFolder;
                    chdir $1 and $jbzDir = decode_utf8 getcwd();
                }
                require FileMgt106::ScanMasterAperture;
                $_->updateJbz( $hints, $jbzDir )
                  foreach FileMgt106::ScanMasterAperture
                  ->findOrMakeApertureLibraries( $hints, @_ );
                last;
            }
            elsif (/^-+migrate(?:=(.+))?/) {
                $self->migrate( undef, $1 );
                next;
            }
            if ( /^-+$/ && $filterFlag ) {
                unless ($filter) {
                    require FileMgt106::CLI::Miscellaneous;
                    $filter =
                      FileMgt106::CLI::Miscellaneous::makeHintsFilterQuick(
                        $hints, $filterFlag );
                    $filter->($_) foreach @baseScalars;
                }
                local undef $/;
                binmode STDIN;
                binmode STDOUT;
                print encode_json( $filter->( decode_json(<STDIN>) ) || {} );
                next;
            }
            if (/^-+missing=(.*)/) {
                chdir $startFolder;
                $missing = FileMgt106::LoadSave::loadNormalisedScalar($1);
                push @grabSources, '';
                next;
            }
            if (/^-+known=(.*)/) {
                $filterFlag ||= 'known';
                undef $filter;
                push @baseScalars,
                  FileMgt106::LoadSave::loadNormalisedScalar($1);
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
                  FileMgt106::LoadSave::loadNormalisedScalar( $root . $ext );
                $target = FileMgt106::LoadSave::parseText( $root . $ext )
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
                $processScalar->( $target, $root, $ext, \@argumentStat );
            }
            elsif ( -d _ && chdir $_ ) {
                $processCwd->(@scanMasterConfigClosures);
            }
            else {
                warn "Ignored: $_";
            }

        }
    };

    my $chooserMaker = sub {
        my ($caseidRoot) = @_;
        return \&_chooserNoCaseids unless $caseidRoot;
        return \&_chooserNoCaseids
          unless $hints ||=
          FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) );
        $hints->beginInteractive;
        my $caseidMap = $hints->{childrenSha1}->($caseidRoot);
        $hints->commit;
        return \&_chooserNoCaseids unless %$caseidMap;
        sub {
            my ( $catalogue, $canonical, $fileExtension ) = @_;
            unlink $canonical . $fileExtension;
            my $target = FileMgt106::LoadSave::loadNormalisedScalar(
                $catalogue,
                sub {
                    $_[0] !~ /^~WRL[0-9]+\.tmp$/s
                      and $_[0] !~ /\.dta$/s;
                }
            );
            delete $target->{$_} foreach grep { /\//; } keys %$target;
            my @caseids = FileMgt106::ScanMaster::extractCaseids($target);
            foreach my $caseid (@caseids) {
                foreach my $folder ( keys %$caseidMap ) {
                    next unless $caseidMap->{$folder} eq $caseid;
                    $folder =~ s#//[0-9]+$##s;
                    next unless -d $folder;
                    my $destination = catdir( $folder, $canonical );
                    lstat $canonical;
                    unlink $canonical if -l _;
                    rename $canonical, $destination if -d _;
                    symlink $destination, $canonical;
                    return ( $target, $destination );
                }
            }
            if ( !-d $canonical ) {
                symlink rel2abs($catalogue), $canonical . $fileExtension;
                return;
            }
            $target, $canonical;
        };
    };

    $processScalar, $processCwd, $finish, $processLegacyArguments,
      $chooserMaker;

}

sub _chooserNoCaseids {
    my ( $catalogue, $canonical, $fileExtension ) = @_;
    unlink $canonical . $fileExtension;
    if ( !-d $canonical ) {
        symlink rel2abs($catalogue), $canonical . $fileExtension;
        return;
    }
    my $target = FileMgt106::LoadSave::loadNormalisedScalar(
        $catalogue,
        sub {
            $_[0] !~ /^~WRL[0-9]+\.tmp$/s
              and $_[0] !~ /\.dta$/s;
        }
    );
    delete $target->{$_} foreach grep { /\//; } keys %$target;
    $target, $canonical;
}

1;
