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
binmode STDERR, ':utf8';
use Encode 'decode_utf8';
use JSON;
require POSIX;

use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename qw(dirname basename);
use Cwd;
my ( $startFolder, $perl5dir );

BEGIN {
    $SIG{INT} = $SIG{USR1} = $SIG{USR2} = sub {
        my ($sig) = @_;
        die "Died on $sig signal\n";
    };
    $startFolder = getcwd();
    $perl5dir = dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
    while (1) {
        last if -d catdir( $perl5dir, 'FileMgt106' );
        my $parent = dirname $perl5dir;
        last if $parent eq $perl5dir;
        $perl5dir = $parent;
    }
    chdir $perl5dir or die "chdir $perl5dir: $!";
    $perl5dir = getcwd();
    chdir $startFolder;
}
use lib $perl5dir;
use FileMgt106::Database;
use FileMgt106::Scanner;
use FileMgt106::Tools;
use FileMgt106::ScanMaster;
use FileMgt106::FileSystem;

my (
    $hints,                 $filter,       @baseScalars,
    $missing,               %scanners,     $grabFrom,
    @applyScanMasterConfig, $cleaningFlag, $filterFlag,
    $syncDestination,
);

foreach (@ARGV) {
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
        @baseScalars = map { FileMgt106::Tools::loadJbz($_); } split /:/, $1;
        next;
    }
    elsif (/^-+grab=?(.*)/) {
        $grabFrom = $1 || '';
        next;
    }
    elsif (/^-+read-?only/) {
        push @applyScanMasterConfig, sub {
            $_[0]->setFrotl(
                2_000_000_000    # This will go wrong in 2033
            );
        };
        next;
    }
    elsif (/^-+cat/) {
        push @applyScanMasterConfig, sub {
            $_[0]->addScalarTaker(
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
        if ( /^auto$/ && chdir catdir( dirname($perl5dir), '~$' ) ) {
            my $repoDir = getcwd();
            push @applyScanMasterConfig, sub {
                my ( $scanner, $dir ) = @_;
                if (   $repoDir
                    && $dir !~ m#/\~\$#
                    && substr( $dir, 0, length($repoDir) ) ne $repoDir )
                {
                    my $repo = $hints->{repositoryPath}->( $dir, $repoDir );
                    $scanner->setRepo($repo)->setCatalogue( $repo, '../%jbz' );
                }
            };
        }
        else {
            my $repoDir = !$_ ? $startFolder : m#^/#s ? $_ : "$startFolder/$_";
            push @applyScanMasterConfig,
              sub { $_[0]->setCatalogue( undef, $repoDir ); };
        }
        next;
    }
    $hints ||=
      FileMgt106::Database->new( catfile( dirname($perl5dir), '~$hints' ) )
      unless $filterFlag && $filterFlag =~ /nodb/i;
    if (/^-+aperture=?(.*)/) {
        my $jbzDir = $startFolder;
        if ($1) {
            chdir $startFolder;
            chdir "$1" and $jbzDir = decode_utf8 getcwd();
        }
        require FileMgt106::ScanAperture;
        $_->updateJbz( $hints, $startFolder, $jbzDir )
          foreach FileMgt106::ScanAperture->libraries( $hints, @ARGV );
        last;
    }
    elsif (/^-+migrate(?:=(.+))?/) {
        my $oldFileName;
        $oldFileName = rel2abs( $1, $startFolder ) if $1;
        chdir dirname($perl5dir) or die "chdir dirname($perl5dir): $!";
        unless ( $oldFileName && -f $oldFileName ) {
            $hints->{dbHandle}->do('begin exclusive transaction')
              or die 'Cannot secure exclusive access to the database';
            $hints->commit;
            delete $hints->{$_} foreach keys %$hints;
            my $mtime = ( stat '~$hints' )[STAT_MTIME]
              or die 'No existing hints file?';
            $mtime =
              POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z', localtime($mtime) );
            $oldFileName = '~$hints ' . $mtime;
            if (undef) {
                system qw(cp -p --), '~$hints', $oldFileName
                  and die "Cannot copy hints file to $oldFileName: $!";
                rename '~$hints', '~$hints-old'
                  or die "Cannot move ~\$hints to ~\$hints-old: $!";
            }
            else {
                rename '~$hints', $oldFileName
                  or die "Cannot move ~\$hints to $oldFileName: $!";
            }
        }
        $hints = FileMgt106::Database->new('~$hints')
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
                $db->do('update t1 set nr=nl where nr is null') unless $_;
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
            next;
        }
        else {
            warn 'New database in use: no migration done';
            next;
        }
    }
    if ( /^-+$/ && $filterFlag ) {
        unless ($filter) {
            $filter = FileMgt106::Tools::makeHintsFilter( $hints, $filterFlag );
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
        $missing = FileMgt106::Tools::loadJbz($1);
        $grabFrom ||= '';
        next;
    }
    if (/^-+known=(.*)/) {
        $filterFlag ||= 'known';
        undef $filter;
        push @baseScalars, FileMgt106::Tools::loadJbz($1);
        next;
    }
    $_ = "$startFolder/$_" unless m#^/#s;
    my @stat = lstat;
    if ( -l _ ) {
        if ( readlink =~ /([0-9a-zA-Z]{40})/ ) {
            $missing->{$_} = $1;
            next;
        }
        @stat = stat;
    }
    if (
        -f _
        && ( my ( $root, $ext ) =
            /(.*)(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$/si )
      )
    {
        my $target;
        $target = FileMgt106::Tools::loadJbz(
            $root . $ext,
            $filter ? undef : sub {
                $_[0] !~ /^~WRL[0-9]+\.tmp$/s
                  and $_[0] !~ /\.dta$/s;
            }
        );
        $target = FileMgt106::Tools::parseText( $root . $ext )
          if !$target && $ext =~ /txt|yml/i;
        if ( $filterFlag && $filterFlag =~ /split/ ) {
            while ( my ( $k, $v ) = each %$target ) {
                local $_ = $k;
                s#/#..#g;
                FileMgt106::Tools::saveJbzPretty( "$root.$_.jbz",
                    ref $v ? $v : { $k => $v } );
            }
            next;
        }
        elsif ( $filterFlag && $filterFlag =~ /explode/ ) {
            FileMgt106::Tools::saveJbzPretty( "$root.exploded.jbz",
                FileMgt106::Tools::explodeByType($target) );
            next;
        }
        my $dir = $root;
        if ($filterFlag) {
            unless ($filter) {
                $filter =
                  FileMgt106::Tools::makeHintsFilter( $hints, $filterFlag );
                $filter->($_) foreach @baseScalars;
            }
            warn "Filtering '$root$ext' with $filterFlag";
            $target = $filter->($target);
            undef $filter if $filterFlag =~ /separate/i;
        }
        elsif ( $dir =~ /(.+)\+missing$/s && -d $1 ) {
            my $rgid = ( stat _ )[STAT_GID];
            chdir $1 or die "chdir $1: $!";
            $dir = decode_utf8 getcwd();
            warn "Infilling $dir with rgid=$rgid and $ext file";
            $hints->beginInteractive;
            eval {
                $target = (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints,
                        $hints->statFromGid( ( stat $dir )[STAT_GID] )
                    )
                )->infill($target);
            };
            warn "infill $dir: $@" if $@;
            $hints->commit;
        }
        else {
            my $rgid = $stat[STAT_GID];
            mkdir $dir unless -e $dir;
            chown 0, $rgid, $dir;
            chmod 0770, $dir;
            chdir $dir or die "chdir $dir: $!";
            $dir = decode_utf8 getcwd();
            warn "Rebuilding $dir with rgid=$rgid and $ext file";
            $hints->beginInteractive;
            eval {
                (
                    $scanners{$dir} = FileMgt106::Scanner->new(
                        $dir, $hints, $hints->statFromGid($rgid)
                    )
                )->scan( 0, $target );
            };
            warn "scan $dir: $@" if $@;
            $hints->commit;
            utime time, $stat[STAT_MTIME], $dir;
        }
        my $missingFile = "$root+missing.jbz";
        $missingFile =~ s/\+missing\+missing/+missing/;
        unlink $missingFile;
        if ( ref $target && keys %$target ) {
            if ( defined $grabFrom ) {
                $missing->{$dir} = $target;
            }
            else {
                FileMgt106::Tools::saveJbz( $missingFile, $target );
            }
        }
    }
    elsif ( -d _ && chdir $_ ) {
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
            FileMgt106::Scanner->new( $destination, $hints, @extrasDestination )
              ->scan( 0, $s );
            $hints->commit;
            next;
        }
        if ($cleaningFlag) {
            if ( $cleaningFlag =~ /dayfolder/i ) {
                warn "One folder per day for files in $dir";
                FileMgt106::Tools::categoriseByDay($dir);
            }
            if ( $cleaningFlag =~ /datemark/i ) {
                warn "Datemarking $dir";
                FileMgt106::Tools::datemarkFolder($dir);
            }
            if ( $cleaningFlag =~ /restamp/i ) {
                warn "Re-timestamping $dir";
                FileMgt106::Tools::restampFolder($dir);
            }
            if ( $cleaningFlag =~ /flat/i ) {
                warn "Flattening $dir";
                $hints->beginInteractive(1);
                FileMgt106::Tools::flattenCwd;
                $hints->commit;
            }
            if ( $cleaningFlag =~ /rename/i ) {
                warn "Renaming in $dir";
                $hints->beginInteractive(1);
                FileMgt106::Tools::normaliseFileNames('.');
                $hints->commit;
            }
            elsif ( $cleaningFlag =~ /clean/i ) {
                warn "Deep cleaning $dir";
                $hints->beginInteractive(1);
                FileMgt106::Tools::deepClean('.');
                $hints->commit;
            }
            next if $cleaningFlag =~ /only/i;
        }
        my $scanner = FileMgt106::ScanMaster->new( $hints, $dir );
        $_->( $scanner, $dir ) foreach @applyScanMasterConfig;
        $scanner->dequeued;
    }
    else {
        warn "Ignored: $_";
    }
}

if ($missing) {
    my ( $cellarScanner, $cellarDir );
    unless ($grabFrom) {
        my $missingFile = "$startFolder/+missing.jbz";
        FileMgt106::Tools::saveJbz( $missingFile, $missing );
        die "Do your own grab: $missingFile";
    }
    unless ( $grabFrom eq 'done' ) {
        $cellarDir = dirname($perl5dir);
        if ( -d ( my $d = $cellarDir . '/~$grab' ) ) {
            $cellarDir = $d;
        }
        $cellarDir .=
            '/Y_Cellar '
          . POSIX::strftime( '%Y-%m-%d %H-%M-%S%z', localtime )
          . ' grabbed';
        mkdir $cellarDir;
        chdir $cellarDir;
        {
            my ( $host, $extract ) =
                $grabFrom =~ /^([a-zA-Z0-9._-]+)$/s ? ( $1, 'extract.pl' )
              : $grabFrom =~ m#^([a-zA-Z0-9._-]+):([ /a-zA-Z0-9._-]+)$#s
              ? ( $1, $2 )
              : die $grabFrom;
            open my $fh, qq^| ssh $host 'perl "$extract" -tar -' | tar -x -f -^;
            binmode $fh;
            print {$fh} encode_json($missing);
        };
        $hints->beginInteractive(1);
        FileMgt106::Tools::deepClean('.');
        $hints->commit;
        $cellarScanner =
          FileMgt106::ScanMaster->new( $hints, decode_utf8 getcwd() );
        $cellarScanner->dequeued;
    }
    while ( my ( $dir, $scalar ) = each %$missing ) {
        $hints->beginInteractive;
        my $stillMissing = eval {
            (
                $scanners{$dir} || FileMgt106::Scanner->new(
                    $dir, $hints,
                    $hints->statFromGid( ( stat $dir )[STAT_GID] )
                )
            )->infill($scalar);
        };
        warn "infill $dir: $@" if $@;
        $hints->commit;
        FileMgt106::Tools::saveJbzPretty( catfile( $dir, "\N{U+26A0}.jbz" ),
            $stillMissing )
          if $stillMissing;
    }
    if ( defined $cellarDir ) {
        $hints->beginInteractive(1);
        FileMgt106::Tools::deepClean($cellarDir);
        $hints->commit;
        $cellarScanner->dequeued;
        rmdir $cellarDir;
    }
}

$hints->disconnect if $hints;
