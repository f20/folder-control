package Daemon112::TopMaster;

# Copyright 2012-2019 Franck Latrémolière and others.
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
use Cwd;
use File::Spec::Functions qw(catdir splitdir);
use Encode qw(decode_utf8);
use FileMgt106::Scanning::ScanMaster;

use constant {
    STAT_DEV => 0,
    STAT_INO => 1,
};

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub attach {

    my ( $topMaster, $root, $runner ) = @_;

    return $topMaster if $topMaster->{'/RESCANNER'};

    $topMaster->{'/RESCANNER'} = sub {

        my ($runner) = @_;
        my $hints = $runner->{hints};
        warn "Scanning $root for $topMaster";
        my @list = $topMaster->_listDirectory($root);
        my %list = map { ( $_ => 1 ); } @list;

        foreach (
            grep {
                !exists $list{$_}
                  && UNIVERSAL::isa( $topMaster->{$_},
                    'FileMgt106::Scanning::ScanMaster' );
            } keys %$topMaster
          )
        {
            my $toBeDeleted = delete $topMaster->{$_};
            $toBeDeleted->takeScalar($runner);
            $toBeDeleted->unwatchAll;
            warn "Stopped watching $toBeDeleted";
        }
        $hints->enqueue(
            $runner->{qu},
            sub {
                my ($hints) = @_;
                my $doclocid =
                  $hints->{topFolder}
                  ->( $root, ( stat $root )[ STAT_DEV, STAT_INO ] );
                die unless $doclocid;
                my $oldChildrenHashref = $hints->{children}->($doclocid);
                my @expected           = keys %$oldChildrenHashref;
                $hints->{uproot}->( $oldChildrenHashref->{$_} )
                  foreach grep { !exists $list{$_} } @expected;
            }
        );

        my $time;
        foreach (@list) {
            my $dir = catdir( $root, $_ );
            if ( -l $dir || !-d _ ) {
                warn "Not watching $dir (not a directory)";
                next;
            }
            my $scanMaster = $topMaster->{$_};
            if ( !$scanMaster ) {
                $scanMaster = $topMaster->{$_} =
                  FileMgt106::Scanning::ScanMaster->new( $hints, $dir )
                  ->setRepoloc( $runner->{locs},
                    $topMaster->{'/repolocOptions'} );
                $scanMaster->setWatch( 'Daemon112::Watcher',
                    $topMaster->{'/kq'} )
                  if $topMaster->{'/kq'};
                $topMaster->{'/scanMasterConfig'}->( $scanMaster, $_, $dir )
                  if $topMaster->{'/scanMasterConfig'};
                $time ||= time + 2;
                $runner->{qu}->enqueue( ++$time, $scanMaster );
            }
            elsif ( UNIVERSAL::isa( $scanMaster, __PACKAGE__ ) ) {
                $scanMaster->attach( $dir, $runner );
            }
            else {
                next;
            }
        }

        if ( defined $runner->{locs}{git} && chdir $runner->{locs}{git} ) {
            $ENV{PATH} =
                '/usr/local/bin:/usr/local/git/bin:/usr/bin:'
              . '/bin:/usr/sbin:/sbin:/opt/sbin:/opt/bin';
            if (@list) {
                my @components = splitdir( $hints->{canonicalPath}->($root) );
                map { s#^\.#_#s; s#\.(\S+)$#_$1#s; } @components;
                my $category =
                  join( '.', map { length $_ ? $_ : '_' } @components )
                  || 'No category';
                if ( chdir $category ) {
                    foreach ( split /\n/, decode_utf8(`git ls-files`) ) {
                        s/\.txt$//s;
                        s/^_/./s;
                        next if exists $list{$_};
                        warn "Removing catalogue for $root/$_";
                        unlink "$_.txt";
                        unlink "$runner->{locs}{jbz}/$category/$_.jbz"
                          if defined $runner->{locs}{jbz}
                          && -d $runner->{locs}{jbz};
                        system qw(git rm --cached), "$_.txt";
                        system qw(git commit -q --untracked-files=no -m),
                          "Removing $root/$_";
                    }
                }
                else {
                    warn "Could not find folder $category";
                }
                if ( !$runner->{locs}{gitLastGarbageCollection}
                    || time - $runner->{locs}{gitLastGarbageCollection} >
                    86_100 )
                {
                    warn "Running git gc in $runner->{locs}{git}";
                    system qw(git gc);
                    $runner->{locs}{gitLastGarbageCollection} = time;
                    warn "Finished git gc in $runner->{locs}{git}";
                }
            }
            chdir $root;
        }

    };

    Daemon112::Watcher->new( $topMaster->{'/RESCANNER'}, "Watcher: $root" )
      ->startWatching( $topMaster->{'/kq'}, $root )
      if $topMaster->{'/kq'};

    $runner->{qu}->enqueue( time + 1, $topMaster ) if $runner;

    $topMaster;

}

sub dequeued {
    my ( $topMaster, $runner ) = @_;
    $topMaster->{'/RESCANNER'}->($runner);
    my $time         = time;
    my @refLocaltime = localtime( $time - 17_000 );
    my $nextRun =
      $time -
      int( 600 * ( $refLocaltime[1] / 10 - rand() ) ) +
      3_600 * ( ( ( $refLocaltime[2] < 18 ? 23 : 47 ) - $refLocaltime[2] ) );
    $runner->{qu}->enqueue( $nextRun, $topMaster );
}

sub _listDirectory {
    my ( $topMaster, $dir ) = @_;
    return unless defined $dir && chdir $dir;
    my $handle;
    opendir $handle, '.' or return;
    my @list =
      map  { decode_utf8 $_; }
      grep { !/^(?:\.\.?|\.DS_Store|Icon\r)$/s; } readdir $handle;
    @list = $topMaster->{'/filter'}->(@list) if $topMaster->{'/filter'};
    @list;
}

1;
