package FileMgt106::ScanMaster;

# Copyright 2012-2019 Franck Latrémolière, Reckon LLP.
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

use overload
  '""' => sub { $_[0][0] || $_[0]; },
  '0+' => sub { $_[0] },
  fallback => 1;

use Storable qw(freeze);
use Digest::SHA qw(sha1 sha1_base64);
require POSIX;
use File::Basename qw(basename dirname);
use File::Spec::Functions qw(catdir catfile splitdir);
use FileMgt106::Scanner;
use FileMgt106::FileSystem;

use constant {
    DIR          => 0,
    HINTS        => 1,
    FROTL        => 2,
    QID          => 3,
    REPOPAIR     => 4,
    RESCANTIME   => 5,
    ROOTLOCID    => 6,
    SCALAR       => 7,
    SCALARFILTER => 8,
    SCALARTAKER  => 9,
    SHA1         => 10,
    STASHPAIR    => 11,
    TTR          => 12,
    WATCHERS     => 13,
    WATCHING     => 14,
};

sub new {
    my ( $class, $hints, $dir ) = @_;
    bless [ $dir, $hints ], $class;
}

sub setRepoloc {

    my ( $self, $repolocs ) = @_;

    my ( $repoFolder, $gitFolder, $jbzFolder, $stashFolder, $omitMirrored ) =
      @{$repolocs}{qw(repo git jbz stash omitMirrored)};

    push @{ $self->[SCALARFILTER] }, sub {
        my ( $runner, $scalar ) = @_;
        my %filtered;
        foreach ( keys %$scalar ) {
            $filtered{$_} = $scalar->{$_}
              unless /^\@/s || / \(mirrored from .+\)$/is;
        }
        \%filtered;
      }
      if $omitMirrored;

    my $gid = ( stat( dirname( $self->[DIR] ) ) )[STAT_GID];
    my @components =
      splitdir( $self->[HINTS]->{canonicalPath}->( $self->[DIR] ) );
    map { s#^\.#_#s; s#\.(\S+)$#_$1#s; } @components;
    my $name = pop(@components) || 'No name';
    $name = "_$name" if $name =~ /^\./s;
    my $category = join( '.', map { length $_ ? $_ : '_' } @components )
      || 'No category';

    $self->[STASHPAIR] = [ $stashFolder, $name ] if defined $stashFolder;

    foreach ( grep { defined $_ && !/^\.\.\//s && -d $_; } $repoFolder,
        $gitFolder, $jbzFolder )
    {
        $_ = catdir( $_, $category );
        unless ( -e $_ ) {
            mkdir $_ or warn "mkdir $_: $!";
            chown -1, $gid, $_;
            chmod 02750, $_;
        }
    }

    if ( defined $repoFolder && -d $repoFolder ) {
        $repoFolder = catdir( $repoFolder, $name );
        unless ( -e $repoFolder ) {
            mkdir $repoFolder or warn "mkdir $repoFolder: $!";
            chown -1, $gid, $repoFolder;
            chmod 02750, $repoFolder;
        }
        if ( -d $repoFolder && -w _ ) {
            $self->[REPOPAIR] = [ $repoFolder, 'No date' ];
        }
    }

    unless ( defined $gitFolder && -d $gitFolder ) {
        unless ( defined $jbzFolder && -d $jbzFolder ) {
            return $self;
        }
        return $self->addJbzName("$jbzFolder/$name.jbz");
    }

    $self->addScalarTaker(
        sub {
            my ( $scalar, $blobref, $runner ) = @_;
            my $run = sub {
                my ($hints) = @_;
                if ($scalar) {
                    my $result =
                      $hints->{updateSha1if}
                      ->( $self->[SHA1], $self->[ROOTLOCID] );
                    $hints->commit;
                    return if defined $result && $result == 0;
                }

                # my $pid = fork;
                # return if $pid;
                # POSIX::setsid() if defined $pid;

                if ( chdir $gitFolder ) {
                    $ENV{PATH} =
                        '/usr/local/bin:/usr/local/git/bin:/usr/bin:'
                      . '/bin:/usr/sbin:/sbin:/opt/sbin:/opt/bin';
                    if ($scalar) {
                        warn "Catalogue update for $self";
                        open my $f, '>', "$name.txt.$$";
                        binmode $f;
                        print {$f} $$blobref;
                        close $f;
                        rename "$name.txt.$$", "$name.txt";
                        system qw(git commit -q --untracked-files=no -m),
                          $self->[DIR]
                          if !system qw(git add), "$name.txt"
                          or !system qw(git init)
                          and !system qw(git add), "$name.txt";
                        if ( defined $jbzFolder && -d $jbzFolder ) {
                            system qw(bzip2), "$name.txt";
                            rename "$name.txt.bz2", "$jbzFolder/$name.jbz";
                        }
                    }
                    else {
                        warn "Removing catalogue for $self";
                        unlink "$name.txt";
                        unlink "$jbzFolder/$name.jbz"
                          if defined $jbzFolder && -d $jbzFolder;
                        system qw(git rm --cached), "$name.txt";
                        system qw(git commit -q --untracked-files=no -m),
                          "Removing $self->[DIR]";
                    }
                }
                else {
                    warn "Cannot chdir to $gitFolder: $!";
                }

                # require POSIX and POSIX::_exit(0);
                # die 'This should not happen';

            };
            if ($runner) {
                delete $self->[SCALAR] unless $self->[WATCHING];
                $self->[HINTS]->enqueue( $runner->{pq}, $run );
            }
            else {
                $run->( $self->[HINTS] );
            }
        }
    );

}

sub addJbzName {
    my ( $self, $jbzName ) = @_;
    $self->addScalarTaker(
        sub {
            require FileMgt106::LoadSave;
            FileMgt106::LoadSave::saveBzOctets( $jbzName . $$, ${ $_[1] } );
            if ( my $mtime = ( lstat $jbzName )[STAT_MTIME] ) {
                $mtime =
                  POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z', localtime($mtime) );
                my $njbz = $jbzName;
                $njbz =~ s/.jbz$/ $mtime.jbz/;
                link $jbzName, $njbz;
            }
            rename $jbzName . $$, $jbzName;
        }
    );
}

sub addJbzFolder {
    my ( $self, $jbzFolder ) = @_;
    my $name = basename( $self->[DIR] );
    $name =~ s/^\./_./s;
    $self->addJbzName( catfile( $jbzFolder, "$name.jbz" ) );
}

sub setWatch {
    my $self = shift;
    if ( $_[0] && $_[1] ) {
        $self->[WATCHING] = \@_;
        warn "Started watching $self";
    }
    else {
        delete $self->[WATCHING];
    }
    $self;
}

sub setFrotl {
    $_[0][FROTL] = $_[1];
    $_[0];
}

sub addScalarTaker {
    my $self = shift;
    push @{ $self->[SCALARTAKER] }, @_;
    delete $self->[SHA1];
    $self;
}

sub setToRescan {
    delete $_[0][RESCANTIME];
    $_[0];
}

sub dequeued {

    my ( $self, $runner ) = @_;
    delete $self->[QID];
    my $time         = time;
    my @refLocaltime = localtime( $time - 17_000 );

    unless ( $self->[SCALAR]
        && $self->[RESCANTIME]
        && $self->[RESCANTIME] > $time )
    {
        $self->unwatchAll;
        chdir $self->[DIR] or die "Cannot chdir to $self->[DIR]: $!";
        my $rgid = ( stat '.' )[STAT_GID];
        my $frotl =
            defined $self->[FROTL] ? $self->[FROTL]
          : $rgid < 500            ? 0
          : $self->[DIR] =~ m#/(\~\$|Y_)#i
          ? 2_000_000_000    # This will go wrong in 2033
          : $self->[DIR] =~ m#/X_#i ? -13            # 13 seconds
          :                           -4_233_600;    # Seven weeks
        $frotl += $time if $frotl < 0;
        $self->[REPOPAIR][1] = POSIX::strftime( '%Y-%m-%d', @refLocaltime )
          if $self->[REPOPAIR];
        warn join ' ', "rgid=$rgid", "timelimit=$frotl", $self->[DIR], "\n";
        my $run = sub {
            my ($hints) = @_;
            @{$self}[ SCALAR, ROOTLOCID ] =
              FileMgt106::Scanner->new( $self->[DIR], $hints,
                $hints->statFromGid($rgid) )
              ->scan( $frotl, undef, $self->[STASHPAIR], $self->[REPOPAIR],
                $self->[WATCHING] ? $self : undef );
            $self->schedule( $time, $runner->{qu} ) if $runner;
        };
        if ($runner) {
            $self->[HINTS]->enqueue( $runner->{pq}, $run );
            my $leftInDay =
              3_600 * ( 24 - $refLocaltime[2] ) -
              $refLocaltime[0] -
              $refLocaltime[1] * 60;
            $self->[RESCANTIME] =
              $time + ( $leftInDay > 7_200 ? 7_200 : $leftInDay );
            return $self;
        }
        else {
            $self->[HINTS]->beginInteractive;
            eval { $run->( $self->[HINTS] ); };
            warn "scan $self->[DIR]: $@" if $@;
            $self->[HINTS]->commit;
        }
    }

    $self->schedule(
        $time - int( 600 * ( $refLocaltime[1] / 10 - rand() ) ) + 3_600 * (
            $self->[WATCHING]
            ? (
                $refLocaltime[6] == 6
                  || $refLocaltime[6] == 0 || $refLocaltime[2] > 19
                ? 24 - $refLocaltime[2]
                : 4
              )
            : ( ( $refLocaltime[2] < 18 ? 23 : 47 ) - $refLocaltime[2] )
        ),
        $runner->{qu}
    ) if $runner;

    $self->takeScalar( $runner, $self->[SCALAR] );

    $self;

}

sub takeScalar {
    my ( $self, $runner, $scalar ) = @_;
    if ( $scalar && $self->[SCALARFILTER] ) {
        $scalar = $_->( $runner, $scalar ) foreach @{ $self->[SCALARFILTER] };
    }
    if ( $self->[SCALARTAKER] ) {
        my ( $blob, $newSha1 );
        if ($scalar) {
            require FileMgt106::LoadSave;
            $blob = FileMgt106::LoadSave::jsonMachineMaker()->encode($scalar);
            $newSha1 = sha1($blob);
        }
        unless ( defined $newSha1
            && defined $self->[SHA1]
            && $self->[SHA1] eq $newSha1 )
        {
            $self->[SHA1] = $newSha1;
            $_->( $scalar, \$blob, $runner ) foreach @{ $self->[SCALARTAKER] };
        }
    }

}

sub schedule {
    my ( $self, $ttr, $queue ) = @_;
    if ( exists $self->[QID] ) {
        delete $self->[QID]
          if $self->[TTR] > $ttr
          and !$queue->set_priority(
            $self->[QID],
            sub { $_[0] == $self },
            $self->[TTR] = $ttr
          );
    }
    $self->[QID] = $queue->enqueue( $self->[TTR] = $ttr, $self )
      unless exists $self->[QID];
    $self;
}

sub watchFolder {
    my (
        $self,  $scanDir, $locid,    $path,     $hashref,
        $frotl, $stasher, $backuper, $priority, $whatToWatch
    ) = @_;
    $whatToWatch ||= '.';
    $frotl = -13 if $frotl && $frotl > time - 300;

    # A controller rescans a single folder, but can be triggered
    # by changes to several files and folders.
    my ( $controller, $frozensha1 );
    $controller = $self->[WATCHERS]{ $path . '.' }
      || $self->[WATCHING][0]->new(
        sub {
            my ($runner) = @_;
            $controller->stopWatching( $self->[WATCHING][1] );
            $self->[HINTS]->enqueue(
                $runner->{pq},
                sub {
                    eval {
                    # Validate locid as well as folder existence before running.
                        my $fullPath = $self->[HINTS]{pathFromLocid}->($locid)
                          or die "No path for locid=$locid";
                        chdir $fullPath or die "Could not chdir $fullPath";
                        $frozensha1 ||= sha1_base64( freeze($hashref) );
                        $scanDir->(
                            $locid,
                            $path,    # relative to $self->[DIR]
                            !$frotl      ? 0
                            : $frotl < 0 ? ( time - $frotl )
                            : $frotl,
                            $self,
                            $hashref,
                            $stasher,
                            $backuper
                        );
                    };
                    if ($@) {
                        warn "$@ in controller for $self";
                        $self->setToRescan->schedule( time + 2, $runner->{qu} );
                        return;
                    }
                    my $newsha1 = sha1_base64( freeze($hashref) );
                    if ( $newsha1 ne $frozensha1 ) {
                        $frozensha1 = $newsha1;
                        $self->schedule( time + 5, $runner->{qu} );
                    }
                }
            );
        },
        "Watcher: $self->[DIR]/$path",
        $self->[WATCHING][2],
      );
    $controller->startWatching( $self->[WATCHING][1], $whatToWatch, $priority );
    $self->[WATCHERS]{ $path . $whatToWatch } = $controller;
}

sub watchFile {
    my ( $self, $scanDir, $locid, $path, $hashref, $name, $stasher, $backuper )
      = @_;
    $self->watchFolder(
        $scanDir,  $locid, $path, $hashref, 0, $stasher,
        $backuper, -10,    $name
    );
}

sub unwatchAll {
    my ($self) = @_;
    $_->stopWatching( $self->[WATCHING][1] )
      foreach values %{ $self->[WATCHERS] };
    $self->[WATCHERS] = {};
    $self;
}

sub extractCaseids {
    my ($hashref) = @_;
    return unless $hashref;
    my @caseids;
    while ( my ( $k, $v ) = each %$hashref ) {
        if ( 'HASH' eq ref $v ) {
            push @caseids, extractCaseids($v);
        }
        elsif ( $k =~ /\.caseid$/is && $v =~ /^[0-9a-f]{40}$/is ) {
            push @caseids, pack( 'H*', $v );
        }
    }
    sort @caseids;
}

1;
