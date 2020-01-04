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
use FileMgt106::FileSystem qw(STAT_GID STAT_MTIME);

use constant {
    SM_DIR          => 0,
    SM_HINTS        => 1,
    SM_FS           => 2,
    SM_FROTL        => 3,
    SM_QID          => 4,
    SM_REPOPAIR     => 5,
    SM_FULLRESCAN   => 6,
    SM_ROOTLOCID    => 7,
    SM_SCALAR       => 8,
    SM_SCALARFILTER => 9,
    SM_SCALARTAKER  => 10,
    SM_SHA1         => 11,
    SM_STASHPAIR    => 12,
    SM_TTR          => 13,
    SM_WATCHERS     => 14,
    SM_WATCHING     => 15,
    SM_RGID         => 16,
};

sub new {
    my ( $class, $hints, $dir, $fs ) = @_;
    bless [ $dir, $hints, $fs || FileMgt106::FileSystem->new ], $class;
}

sub setRepoloc {

    my ( $self, $repolocs, $options ) = @_;
    my ( $repoFolder, $gitFolder, $jbzFolder, $stashFolder, $resolveFlag ) =
      @{$repolocs}{qw(repo git jbz stash resolve)};
    undef $repoFolder if $options->{omitRepo};

    my $gid = ( stat( dirname( $self->[SM_DIR] ) ) )[STAT_GID];
    my @components =
      splitdir( $self->[SM_HINTS]->{canonicalPath}->( $self->[SM_DIR] ) );
    map { s#^\.#_.#s; } @components;
    my $name = pop(@components) || 'No name';
    my $category = join( '.', map { length $_ ? $_ : '_' } @components )
      || 'No category';

    $self->[SM_STASHPAIR] = [ $stashFolder, $name ] if defined $stashFolder;

    foreach ( grep { defined $_ && !/^\.\.\//s && -d $_; } $repoFolder,
        $gitFolder, $jbzFolder )
    {
        $_ = catdir( $_, $category );
        unless ( -e $_ ) {
            mkdir $_ or warn "mkdir $_: $!";
            chown -1, $gid, $_;
            chmod 02770, $_;
        }
    }

    if ($resolveFlag) {
        require FileMgt106::ResolveFilter;
        $self->addScalarFilter(
            sub {
                my ( $runner, $scalar ) = @_;
                FileMgt106::ResolveFilter::resolveAbsolutePaths(
                    $scalar,
                    $self->[SM_HINTS]{sha1FromStat},
                    \&FileMgt106::Scanner::sha1File
                );
            }
        );
    }

    if ( defined $repoFolder && -d $repoFolder ) {
        $repoFolder = catdir( $repoFolder, $name );
        unless ( -e $repoFolder ) {
            mkdir $repoFolder or warn "mkdir $repoFolder: $!";
            chown -1, $gid, $repoFolder;
            chmod 02770, $repoFolder;
        }
        if ( -d $repoFolder && -w _ ) {
            $self->[SM_REPOPAIR] = [ $repoFolder, 'No date' ];
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
                      ->( $self->[SM_SHA1], $self->[SM_ROOTLOCID] );
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
                        # implement keepCatalogueSymlink?
                        system qw(git commit -q --untracked-files=no -m),
                          $self->[SM_DIR]
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
                          "Removing $self->[SM_DIR]";
                    }
                }
                else {
                    warn "Cannot chdir to $gitFolder: $!";
                }

                # require POSIX and POSIX::_exit(0);
                # die 'This should not happen';

            };
            if ($runner) {
                delete $self->[SM_SCALAR] unless $self->[SM_WATCHING];
                $self->[SM_HINTS]->enqueue( $runner->{pq}, $run );
            }
            else {
                $run->( $self->[SM_HINTS] );
            }
        }
    );

}

sub addJbzName {
    my ( $self, $jbzName ) = @_;
    $self->addScalarTaker(
        sub {
            require FileMgt106::LoadSaveNormalize;
            FileMgt106::LoadSaveNormalize::saveBzOctets( $jbzName . $$,
                ${ $_[1] } );
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
    my $name = basename( $self->[SM_DIR] );
    $name =~ s/^\./_./s;
    $self->addJbzName( catfile( $jbzFolder, "$name.jbz" ) );
}

sub setWatch {
    my $self = shift;
    if ( $_[0] && $_[1] ) {
        $self->[SM_WATCHING] = \@_;
        warn "Started watching $self";
    }
    else {
        delete $self->[SM_WATCHING];
    }
    $self;
}

sub setFrotl {
    $_[0][SM_FROTL] = $_[1];
    $_[0];
}

sub prohibitActions {
    $_[0][SM_RGID] = 0;
    $_[0];
}

sub addScalarTaker {
    my $self = shift;
    push @{ $self->[SM_SCALARTAKER] }, @_;
    delete $self->[SM_SHA1];
    $self;
}

sub addScalarFilter {
    my $self = shift;
    push @{ $self->[SM_SCALARFILTER] }, @_;
    $self;
}

sub setToRescan {
    delete $_[0][SM_FULLRESCAN];
    $_[0];
}

sub scan {
    my ( $self, $hints, $rgid, $frotl ) = @_;
    @{$self}[ SM_SCALAR, SM_ROOTLOCID ] =
      FileMgt106::Scanner->new( $self->[SM_DIR], $hints,
        $self->[SM_FS]->statFromGid($rgid) )
      ->scan( $frotl, undef, $self->[SM_STASHPAIR], $self->[SM_REPOPAIR],
        $self->[SM_WATCHING] ? $self : undef );
}

sub dequeued {

    my ( $self, $runner ) = @_;
    delete $self->[SM_QID];
    my $time         = time;
    my @refLocaltime = localtime( $time - 17_000 );

    unless ( $self->[SM_SCALAR]
        && $self->[SM_FULLRESCAN]
        && $self->[SM_FULLRESCAN] > $time )
    {
        $self->unwatchAll;
        chdir $self->[SM_DIR] or die "Cannot chdir to $self->[SM_DIR]: $!";
        my $rgid =
          defined $self->[SM_RGID] ? $self->[SM_RGID] : ( stat '.' )[STAT_GID];
        my $frotl =
            defined $self->[SM_FROTL] ? $self->[SM_FROTL]
          : $rgid < 500               ? 0
          : $self->[SM_DIR] =~ m#/(\~\$|Y_)#i
          ? 2_000_000_000    # This will go wrong in 2033
          : $self->[SM_DIR] =~ m#/X_#i ? -13            # 13 seconds
          :                              -4_233_600;    # Seven weeks
        $frotl += $time if $frotl < 0;
        $self->[SM_REPOPAIR][1] = POSIX::strftime( '%Y-%m-%d', @refLocaltime )
          if $self->[SM_REPOPAIR];
        warn join ' ', "rgid=$rgid", "timelimit=$frotl", $self->[SM_DIR], "\n";
        my $run = sub {
            my ($hints) = @_;
            $self->scan( $hints, $rgid, $frotl );
            $self->schedule( $time, $runner->{qu} ) if $runner;
        };
        if ($runner) {
            $self->[SM_HINTS]->enqueue( $runner->{pq}, $run );
            $self->[SM_FULLRESCAN] =
              $time + $self->fullRescanTimeOffset(@refLocaltime);
            return $self;
        }
        else {
            $self->[SM_HINTS]->beginInteractive;
            eval { $run->( $self->[SM_HINTS] ); };
            warn "scan $self->[SM_DIR]: $@" if $@;
            $self->[SM_HINTS]->commit;
        }
    }

    $self->schedule(
        $time - int( 600 * ( $refLocaltime[1] / 10 - rand() ) ) + 3_600 * (
            $self->[SM_WATCHING]
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

    $self->takeScalar( $runner, $self->[SM_SCALAR] );

    $self;

}

sub takeScalar {
    my ( $self, $runner, $scalar ) = @_;
    if ( $scalar && $self->[SM_SCALARFILTER] ) {
        $scalar = $_->( $runner, $scalar )
          foreach @{ $self->[SM_SCALARFILTER] };
    }
    if ( $self->[SM_SCALARTAKER] ) {
        my ( $blob, $newSha1 );
        if ($scalar) {
            require FileMgt106::LoadSaveNormalize;
            $blob = FileMgt106::LoadSaveNormalize::jsonMachineMaker()
              ->encode($scalar);
            $newSha1 = sha1($blob);
        }
        unless ( defined $newSha1
            && defined $self->[SM_SHA1]
            && $self->[SM_SHA1] eq $newSha1 )
        {
            $self->[SM_SHA1] = $newSha1;
            $_->( $scalar, \$blob, $runner )
              foreach @{ $self->[SM_SCALARTAKER] };
        }
    }
}

sub schedule {
    my ( $self, $ttr, $queue ) = @_;
    if ( exists $self->[SM_QID] ) {
        delete $self->[SM_QID]
          if $self->[SM_TTR] > $ttr
          and !$queue->set_priority(
            $self->[SM_QID],
            sub { $_[0] == $self },
            $self->[SM_TTR] = $ttr
          );
    }
    $self->[SM_QID] = $queue->enqueue( $self->[SM_TTR] = $ttr, $self )
      unless exists $self->[SM_QID];
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
    $controller = $self->[SM_WATCHERS]{ $path . '.' }
      || $self->[SM_WATCHING][0]->new(
        sub {
            my ($runner) = @_;
            $controller->stopWatching( $self->[SM_WATCHING][1] );
            $self->[SM_HINTS]->enqueue(
                $runner->{pq},
                sub {
                    eval {
                    # Validate locid as well as folder existence before running.
                        my $fullPath =
                          $self->[SM_HINTS]{pathFromLocid}->($locid)
                          or die "No path for locid=$locid";
                        chdir $fullPath or die "Could not chdir $fullPath";
                        $frozensha1 ||= sha1_base64( freeze($hashref) );
                        $scanDir->(
                            $locid,
                            $path,    # relative to $self->[SM_DIR]
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
        "Watcher: $self->[SM_DIR]/$path",
        $self->[SM_WATCHING][2],
      );
    $controller->startWatching( $self->[SM_WATCHING][1],
        $whatToWatch, $priority );
    $self->[SM_WATCHERS]{ $path . $whatToWatch } = $controller;
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
    $_->stopWatching( $self->[SM_WATCHING][1] )
      foreach values %{ $self->[SM_WATCHERS] };
    $self->[SM_WATCHERS] = {};
    $self;
}

sub fullRescanTimeOffset {
    my ( $self, @refLocaltime ) = @_;
    my $leftInDay =
      3_600 * ( 24 - $refLocaltime[2] ) -
      $refLocaltime[0] -
      $refLocaltime[1] * 60;
    $leftInDay > 7_200 ? 7_200 : $leftInDay;
}

1;
