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

sub activate {
    my ( $master, $hints, $runner, $repoDir, $timeref ) = @_;
    my $root = getcwd();
    my $pq   = delete $master->{'/pq'} || $runner->{pq} || $runner->{qu};
    my $kq   = delete $master->{'/kq'};

    if ($kq) {
        if ( my $taker = delete $master->{'/taker'} )
        {    # watch ourselves with custom catalogue taker
            my $repo = $hints->{repositoryPath}->( $root, $repoDir );
            $pq->enqueue(
                time + 1,
                FileMgt106::ScanMaster->new( $hints, $root )->setRepo($repo)
                  ->setCatalogue($repo)->setWatch( 'Daemon112::Watcher', $kq )
                  ->setToRescan->setScalarTaker($taker)
            );
            return;
        }

        # else watch each of our children
        my $controller = Daemon112::Watcher->new(
            sub {
                my ($runner) = @_;
                my $time = time;
                my $dh;
                opendir $dh, $root;
                my @list = readdir $dh;
                closedir $dh;
                @list = $master->{'/filter'}->(@list) if $master->{'/filter'};
                my %list =
                  map { ( $_ => 1 ); }
                  grep { !/^\.\.?$/s && -d "$root/$_"; } @list;

                foreach (
                    grep {
                        !exists $list{$_}
                          && UNIVERSAL::isa( $master->{$_},
                            'FileMgt106::ScanMaster' );
                    } keys %$master
                  )
                {
                    $master->{$_}->unwatchAll;
                    delete $master->{$_};
                }
                $hints->enqueue(
                    $runner->{qu},
                    sub {
                        my ($hints) = @_;
                        my $doclocid =
                          $hints->{topFolder}
                          ->( $root, ( stat $root )[ STAT_DEV, STAT_INO ] );
                        die unless $doclocid;
                        my $oldChildrenHashref =
                          $hints->{children}->($doclocid);
                        $hints->{uproot}->( $oldChildrenHashref->{$_} )
                          foreach grep { !exists $list{$_} }
                          keys %$oldChildrenHashref;
                    }
                );
                foreach ( grep { !exists $master->{$_} } keys %list ) {
                    my $repo =
                      $hints->{repositoryPath}
                      ->( catdir( $root, $_ ), $repoDir );
                    $runner->{qu}->enqueue(
                        ++$time,
                        $master->{$_} =
                          FileMgt106::ScanMaster->new( $hints,
                            catdir( $root, $_ ) )->setRepo($repo)
                          ->setCatalogue( $repo, '../%jbz' )
                          ->setWatch( 'Daemon112::Watcher', $kq )
                    );
                }
            },
            "Watcher: $root"

        );
        $controller->startWatching( $kq, $root );
    }

    # delegate to lower-level TopMaster or ScanMaster objects
    $timeref ||= \( time + 2 );
    my @list;
    {
        my $handle;
        opendir $handle, '.' or return;
        @list =
          map { decode_utf8 $_; }
          grep { !/^\.\.?$/s && !-l $_ && -d _ } readdir $handle;
        @list = $master->{'/filter'}->(@list) if $master->{'/filter'};
    }
    foreach (@list) {
        chdir $_ or do {
            warn "Cannot chdir to $_ in $root: $!";
            next;
        };
        if ( $master->{$_} ) {
            $master->{$_}->activate( $hints, $runner, $repoDir, $timeref )
              if UNIVERSAL::isa( $master->{$_}, __PACKAGE__ );
        }
        else {
            my $dir = decode_utf8 getcwd();
            my $repo = $hints->{repositoryPath}->( $dir, $repoDir );
            $pq->enqueue( ++$$timeref,
                $master->{$_} =
                  FileMgt106::ScanMaster->new( $hints, $dir )->setRepo($repo)
                  ->setCatalogue( $repo, '../%jbz' )
                  ->setWatch( 'Daemon112::Watcher', $kq ) );
        }
        chdir $root or die "Cannot chdir $root: $!";
    }

}

1;
