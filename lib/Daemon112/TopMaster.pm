package Daemon112::TopMaster;

=head Copyright licence and disclaimer

Copyright 2012-2016 Franck Latrémolière and Reckon LLP.

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
use Cwd;
use File::Spec::Functions qw(catdir);
use Encode qw(decode_utf8);
use FileMgt106::ScanMaster;

use constant {
    STAT_DEV => 0,
    STAT_INO => 1,
};

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub dumpState {
    my ( $topMaster, $prefix ) = @_;
    $prefix ||= "$topMaster/";
    warn "TopMaster $prefix " . ( 0 + keys %$topMaster ) . ' keys';
    return;
    foreach ( sort keys %$topMaster ) {
        warn "$prefix$_: $topMaster->{$_}\n";
        $topMaster->{$_}->dumpState("$prefix$_/")
          if UNIVERSAL::can( $topMaster->{$_}, 'dumpState' );
    }
}

sub attach {

    my ( $topMaster, $root ) = @_;

    return $topMaster if $topMaster->{'/RESCANNER'};

    $topMaster->{'/RESCANNER'} = sub {
        my ($runner) = @_;
        if ( my $gitrepo = $runner->{locs}{git} ) {
            if ( !$runner->{locs}{gitLastGarbageCollection}
                || time - $runner->{locs}{gitLastGarbageCollection} > 86000
                and chdir $gitrepo )
            {    # Need a way to detect and remove abandoned catalogue files
                warn "Running git gc in $gitrepo";
                system qw(git gc);
                $runner->{locs}{gitLastGarbageCollection} = time;
                warn "Finished git gc in $gitrepo";
            }
        }
        my $hints = $runner->{hints};
        warn "Scanning $root for $topMaster";
        my @list = $topMaster->_listDirectory($root);
        my %list = map { ( $_ => 1 ); } @list;
        foreach (
            grep {
                !exists $list{$_}
                  && UNIVERSAL::isa( $topMaster->{$_},
                    'FileMgt106::ScanMaster' );
            } keys %$topMaster
          )
        {
            my $toBeDeleted = delete $topMaster->{$_};
            $toBeDeleted->updateCatalogue( undef, undef, $runner );
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
                warn "Cannot watch $dir: not a directory";
                next;
            }
            my $scanMaster = $topMaster->{$_};
            if ( !$scanMaster ) {
                $scanMaster = $topMaster->{$_} =
                  FileMgt106::ScanMaster->new( $hints, catdir( $root, $_ ) )
                  ->setRepoloc( $runner->{locs} );
                $scanMaster->setWatch( 'Daemon112::Watcher',
                    $topMaster->{'/kq'} )
                  if $topMaster->{'/kq'};
                $topMaster->{'/postwatch'}->( $scanMaster, $_ )
                  if $topMaster->{'/postwatch'};
            }
            elsif ( UNIVERSAL::isa( $scanMaster, __PACKAGE__ ) ) {
                $scanMaster->attach( $dir, $runner );
            }
            else {
                next;
            }
            $time ||= time + 2;
            $runner->{qu}->enqueue( ++$time, $scanMaster );
        }
    };

    Daemon112::Watcher->new( $topMaster->{'/RESCANNER'}, "Watcher: $root" )
      ->startWatching( $topMaster->{'/kq'}, $root )
      if $topMaster->{'/kq'};

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
    defined $dir and chdir($dir) || return;
    my $handle;
    opendir $handle, '.' or return;
    my @list =
      map  { decode_utf8 $_; }
      grep { !/^(?:\.\.?|\.DS_Store|Icon\r)$/s; } readdir $handle;
    @list = $topMaster->{'/filter'}->(@list) if $topMaster->{'/filter'};
    @list;
}

1;
