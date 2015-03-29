package Daemon112::TopMaster;

=head Copyright licence and disclaimer

Copyright 2012-2015 Franck Latrémolière, Reckon LLP.

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

sub attach {

    my ( $master, $root, $runner ) = @_;

    my $rescan = $master->{'/RESCANNER'} ||= sub {
        my ($runner) = @_;
        my $hints    = $runner->{hints};
        my @list     = $master->_listDirectory($root);
        my %list = map { ( $_ => 1 ); } @list;
        if (
            my @missing =
            grep {
                !exists $list{$_}
                  && UNIVERSAL::isa( $master->{$_}, 'FileMgt106::ScanMaster' );
            } keys %$master
          )
        {
            $master->{$_}->unwatchAll foreach @missing;
            delete @{$master}{@missing};
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
                    @expected = $master->{'/filter'}->(@expected)
                      if $master->{'/filter'};
                    $hints->{uproot}->( $oldChildrenHashref->{$_} )
                      foreach grep { !exists $list{$_} } @expected;
                }
            );
        }
        my $time;
        foreach (@list) {
            my $dir = catdir( $root, $_ );
            if ( $master->{$_} ) {
                $master->{$_}->attach( $dir, $runner )
                  if UNIVERSAL::isa( $master->{$_}, __PACKAGE__ )
                  and -d $dir;
                next;
            }
            my $repo = $hints->{repositoryPath}->( $dir, $runner->{repoDir} );
            my $scanm = $master->{$_} =
              FileMgt106::ScanMaster->new( $hints, catdir( $root, $_ ) )
              ->setRepo($repo)->setCatalogue( $repo, '../%jbz' );
            $scanm->setWatch( 'Daemon112::Watcher', $master->{'/kq'} )
              if $master->{'/kq'};
            $master->{'/postwatch'}->( $scanm, $_ ) if $master->{'/postwatch'};
            $time ||= time + 2;
            $runner->{qu}->enqueue( ++$time, $scanm );
        }
    };

    Daemon112::Watcher->new( $rescan, "Watcher: $root" )
      ->startWatching( $master->{'/kq'}, $root )
      if $master->{'/kq'};

    $rescan->($runner) if $runner;

    $master;

}

sub dequeued {
    my ( $master, $runner ) = @_;
    $master->{'/RESCANNER'}->($runner);
    my $time         = time;
    my @refLocaltime = localtime( $time - 17_000 );
    my $nextRun =
      $time -
      int( 600 * ( $refLocaltime[1] / 10 - rand() ) ) +
      3_600 * ( ( ( $refLocaltime[2] < 18 ? 23 : 47 ) - $refLocaltime[2] ) );
    $runner->{qu}->enqueue( $nextRun, $master );
}

sub _listDirectory {
    my ( $master, $dir ) = @_;
    defined $dir and chdir($dir) || return;
    my $handle;
    opendir $handle, '.' or return;
    my @list =
      map { decode_utf8 $_; }
      grep { !/^\.\.?$/s && !-l $_ && -d _ } readdir $handle;
    @list = $master->{'/filter'}->(@list) if $master->{'/filter'};
    @list;
}

1;
