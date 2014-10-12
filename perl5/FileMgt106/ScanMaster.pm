package FileMgt106::ScanMaster;

=head Copyright licence and disclaimer

Copyright 2012-2014 Franck Latrémolière, Reckon LLP.

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

use overload
  '""' => sub { $_[0][0] || 'Anonymous ScanMaster ' . ( 0 + $_[0] ); },
  '0+' => sub { $_[0] },
  fallback => 1;

use Storable qw(freeze);
use Digest::SHA qw(sha1 sha1_base64);
require POSIX;
use FileMgt106::Scanner;
use FileMgt106::Tools;
use FileMgt106::FileSystem;
use FileMgt106::Permissions;
use JSON;

use constant {
    DIR         => 0,
    HINTS       => 1,
    REPO        => 2,
    WATCHING    => 3,
    WATCHERS    => 4,
    SCALAR      => 5,
    QID         => 6,
    TTR         => 7,
    RESCANTIME  => 8,
    ROOTLOCID   => 9,
    SHA1        => 10,
    JSONTAKER   => 11,
    JSONFOLDER  => 12,
    FROTL       => 13,
    SCALARTAKER => 14,
};

sub new {
    my ( $class, $hints, $dir ) = @_;
    bless [ $dir, $hints ], $class;
}

sub setRepo {
    my ( $self, $repository ) = @_;
    if ($repository) {
        unless ( -e $repository ) {
            mkdir $repository;
            chmod 02770, $repository;
        }
        if ( -d $repository && -w _ ) {
            $self->[REPO] = [ $repository, 'No date' ];
            $self->[JSONFOLDER] ||= $repository;
        }
    }
    else {
        delete $self->[REPO];
    }
    $self;
}

sub setJsonTaker {
    my ( $self, $jsonTaker, $jsonFolder ) = @_;
    if ($jsonFolder) {
        mkdir $jsonFolder unless -e $jsonFolder;
        if ( -d $jsonFolder && -w _ ) {
            $self->[JSONFOLDER] = $jsonFolder;
        }
    }
    if ( $jsonTaker
        and ref $jsonTaker || -x $jsonTaker )
    {
        $self->[JSONTAKER] = ref $jsonTaker ? $jsonTaker : [$jsonTaker];
        $self->[JSONFOLDER] ||= $self->[DIR];
    }
    else {
        delete $self->[JSONTAKER];
    }
    $self;
}

sub setWatch {
    my $self = shift;
    if ( defined $_[0] ) {
        $self->[WATCHING] = \@_;
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

sub setScalarTaker {
    $_[0][SCALARTAKER] = $_[1];
    delete $_[0][SHA1];
    $_[0];
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
            $rgid < 500                    ? 0
          : $self->[FROTL]                 ? $self->[FROTL]
          : $self->[DIR] =~ m#/(\~\$|Y_)#i ? 2_000_000_000
          : $self->[DIR] =~ m#/X_#i        ? -13
          :                                  -4_233_600;
        $frotl += $time if $frotl < 0;
        $self->[REPO][1] = POSIX::strftime( '%Y-%m-%d', @refLocaltime )
          if ref $self->[REPO];
        warn join ' ', "rgid=$rgid", "timelimit=$frotl", $self->[DIR], "\n";
        my $run = sub {
            my ($hints) = @_;
            @{$self}[ SCALAR, ROOTLOCID ] =
              FileMgt106::Scanner->new( $self->[DIR], $hints,
                statFromGid($rgid) )
              ->scan( $frotl, undef, $self->[REPO],
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
    return $self
      unless $self->[SCALARTAKER]
      or $self->[JSONFOLDER] && -d $self->[JSONFOLDER] && -w _ ;
    my $blob = JSON->new->canonical(1)->utf8->pretty;
    $blob = $blob->encode( $self->[SCALAR] );
    my $newSha1 = sha1($blob);
    unless ( defined $self->[SHA1] && $self->[SHA1] eq $newSha1 ) {
        $self->[SCALARTAKER]->( $self->[SCALAR] ) if $self->[SCALARTAKER];
        return $self
          unless $self->[JSONFOLDER] && -d $self->[JSONFOLDER] && -w _ ;
        my $run = sub {
            my ($hints) = @_;
            my $result =
              $hints->{updateSha1if}
              ->( $self->[SHA1] = $newSha1, $self->[ROOTLOCID] );
            $hints->commit;
            return if defined $result && $result == 0;
            warn "Catalogue update for $self";
            chdir $self->[JSONFOLDER]
              or die "Cannot chdir to $self->[JSONFOLDER]: $!";
            my ($name) = ( $self->[DIR] =~ m#([^/]+)/*$#s );
            $name ||= 'No name';
            $name = "_$name" if $name =~ /^\./s;

            if ( $self->[JSONTAKER] ) {
                open my $f, '>', "$name.txt.$$";
                binmode $f;
                print {$f} $blob;
                close $f;
                rename "$name.txt.$$", "$name.txt";
                system @{ $self->[JSONTAKER] }, $name;
                return;
            }
            FileMgt106::Tools::saveBzOctets( 'tmp.jbz', $blob );
            if ( $self->[REPO] ) {
                my $jbz0 = $name .= '.jbz';
                $name =~
s/.jbz$/POSIX::strftime( ' %Y-%m-%d %H-%M-%S %Z', localtime).'.jbz'/e;
                mkdir $self->[REPO][1] unless -e $self->[REPO][1];
                $name = "$self->[REPO][1]/$name";
                rename 'tmp.jbz', $name;
                link $name, 'tmp.jbz';
                rename 'tmp.jbz', $jbz0;
            }
            else {
                $name = $name . '.jbz';
                if ( my $mtime = ( lstat $name )[STAT_MTIME] ) {
                    $mtime = POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z',
                        localtime($mtime) );
                    my $njbz = $name;
                    $njbz =~ s/.jbz$/ $mtime.jbz/;
                    $njbz = '~$stash/' . $njbz if -e '~$stash';
                    link $name, $njbz;
                }
                rename 'tmp.jbz', $name;
            }
        };
        if ($runner) {
            delete $self->[SCALAR] unless $self->[WATCHING];
            $self->[HINTS]->enqueue( $runner->{pq}, $run );
        }
        else {
            $run->( $self->[HINTS] );
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
    $self;
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

    # A single folder-based controller can watch several files and folders.
    my ( $controller, $frozensha1 );
    $controller = $self->[WATCHERS]{ $path . '.' }
      || $self->[WATCHING][0]->new(
        sub {
            my ($runner) = @_;
            $self->[HINTS]->enqueue(
                $runner->{pq},
                sub {
                    unless ( chdir "$self->[DIR]/$path" ) {
                        $controller->stopWatching( $self->[WATCHING][1] );
                        return;
                    }
                    $frozensha1 ||= sha1_base64( freeze($hashref) );
                    eval {
                        $scanDir->(
                            $locid,
                            $path,
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
        "Watcher: $self->[DIR]/$path"
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
    $_->stopWatching( $self->[WATCHING][1] )->unschedule
      foreach values %{ $self->[WATCHERS] };
    $self->[WATCHERS] = {};
    $self;
}

1;
