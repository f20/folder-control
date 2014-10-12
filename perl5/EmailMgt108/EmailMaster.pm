package EmailMgt108::EmailMaster;

=head Copyright licence and disclaimer

Copyright 2012 Franck Latrémolière, Reckon LLP.

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

use strict;
use warnings;
use utf8;
use JSON;

sub new {
    my ( $class, $runner, $parseCommand ) = @_;
    bless {
        queue        => $runner->{pq},
        parseCommand => $parseCommand,
    }, $class;
}

sub addImapScanMaster {
    my ( $self, $imapScanMaster, $imapFolder, $emailFolder ) = @_;
    $imapScanMaster->setScalarTaker(
        sub {
            $self->{$emailFolder}{$imapFolder} = $_[0];
            $self->schedule;
        }
    );
    $self->schedule;
}

sub schedule {
    my ( $self, $ttr ) = @_;
    $ttr ||= time + 7;
    if ( exists $self->{qid} ) {
        delete $self->{qid}
          unless $self->{ttr} <= $ttr || $self->{queue}->set_priority(
            $self->{qid},
            sub { $_[0] == $self },
            $self->{ttr} = $ttr
          );
    }
    $self->{qid} = $self->{queue}->enqueue( $self->{ttr} = $ttr, $self )
      unless exists $self->{qid};
    $self;
}

sub dequeued {
    my ( $self, $runner ) = @_;
    delete $self->{qid};
    if ( $self->{pid} ) {
        my $pid2 = waitpid( $self->{pid}, POSIX::WNOHANG() );
        if ( $pid2 == $self->{pid} ) {
            delete $self->{pid};
        }
        else {
            warn "Still waiting for $self->{pid}";
            $self->schedule;
            return;
        }
    }
    return unless my @folders = grep { m#^/#s } %$self;
    my @foldersToDo;
    foreach (@folders) {
        chdir $_ or next;
        mkdir '~$ email index $~' unless -e '~$ email index $~';
        chown 60, -1, '~$ email index $~';
        chmod 0770, '~$ email index $~';
        if ( -e '~$ email index $~/scanning' and time - ( stat _ )[9] < 300 ) {
            warn 'Barred by ~$ email index $~/scanning in ' . $_;
            next;
        }
        my $dbh =
          DBI->connect( 'dbi:SQLite:dbname=~$ email index $~/sources.sqlite',
            '', '', { sqlite_unicode => 1, } );
        $dbh->sqlite_busy_timeout(5_000);
        unless ( $dbh->do('begin immediate transaction') ) {
            warn "EmailMaster cannot begin immediate transaction for $_ in "
              . `pwd`;
            next;
        }
        eval { $dbh->do($_) } foreach grep { $_ } split /;\s*/, <<EOS;
create table if not exists repo (
    id integer primary key,
    name text
);
create unique index if not exists reponame on repo (name);
create table if not exists map (
    id integer,
    sha1hex text,
    primary key (id, sha1hex)
);    
EOS
        my $unstashedFolders = {};
        my $stashedFolders   = {};
        my $toScan           = {};
        local undef $/;

        if ( open my $h, '<', '~$ email index $~/unstashed.json' ) {
            binmode $h;
            $unstashedFolders = decode_json(<$h>) || {};
        }
        if ( open my $h, '<', '~$ email index $~/stashed.json' ) {
            binmode $h;
            $stashedFolders = decode_json(<$h>) || {};
        }
        if ( open my $h, '<', '~$ email index $~/toscan.json' ) {
            binmode $h;
            $toScan = decode_json(<$h>) || {};
        }

        my $insertIgnoreRepoName =
          $dbh->prepare('insert or ignore into repo (name) values (?)');
        my $getRepoId = $dbh->prepare('select id from repo where name=?');
        my $deleteRepoIdFromMap = $dbh->prepare('delete from map where id=?');
        my $addToMap =
          $dbh->prepare(
            'insert or ignore into map (id, sha1hex) values (?, ?)');
        my $addScalarToMap = sub {
            my ( $imapRepo, $scalar ) = @_;
            $insertIgnoreRepoName->execute($imapRepo);
            $getRepoId->execute($imapRepo);
            my ($id) = $getRepoId->fetchrow_array;
            $getRepoId->finish;
            $deleteRepoIdFromMap->execute($id);
            $addToMap->bind_param( 1, $id );
            my $makeLoc = sub {
                my ( $parent, $name ) = @_;
                sub {
                    if ( defined $parent ) {
                        $name = $parent->() . "/$name";
                        undef $parent;
                    }
                    $name;
                };
            };
            my $run;
            $run = sub {
                my ( $scalar, $location ) = @_;
                if ( ref $scalar ) {
                    $run->( $scalar->{$_}, $makeLoc->( $location, $_ ) )
                      foreach grep { !/^cyrus\./s } keys %$scalar;
                    return;
                }
                $addToMap->bind_param( 2, $scalar );
                $addToMap->execute;
                return if $unstashedFolders->{$scalar};
                if ( my $folder = $stashedFolders->{$scalar} ) {
                    my $container = substr( $folder, 2, 7 );
                    -e $container or mkdir $container or die;
                    rename '~$ email index $~/' . $folder, "$container/$folder"
                      or die;
                    $unstashedFolders->{$scalar} = "$container/$folder";
                    delete $stashedFolders->{$scalar};
                    return;
                }
                $toScan->{$scalar} = $location->();
            };
            $run->( $scalar, sub { $imapRepo; } );
        };
        while ( my ( $imapRepo, $scalar ) = each %{ $self->{$_} } ) {
            $addScalarToMap->( $imapRepo, $scalar );
        }
        if ( keys %$toScan ) {
            open my $h, '>', '~$ email index $~/toscan.json';
            binmode $h;
            print {$h} encode_json($toScan);
            push @foldersToDo, $_;
        }
        my $keep = $dbh->prepare('select sha1hex from map group by sha1hex');
        $keep->execute;
        my %tos = %$unstashedFolders;
        while ( my ($sha1hex) = $keep->fetchrow_array ) {
            delete $tos{$sha1hex};
        }
        my $status;
        sleep 2 while !( $status = $dbh->commit );
        $dbh->disconnect;
        while ( my ( $sha1hex, $folder ) = each %tos ) {
            my $folder2 = $folder;
            $folder2 =~ s#^.*/##s;
            rename $folder, '~$ email index $~/' . $folder2 or next;
            $stashedFolders->{$sha1hex} = $folder2;
            delete $unstashedFolders->{$sha1hex};
        }
        if ( open my $h, '>', '~$ email index $~/unstashed.json' ) {
            binmode $h;
            print {$h} encode_json($unstashedFolders);
        }
        if ( open my $h, '>', '~$ email index $~/stashed.json' ) {
            binmode $h;
            print {$h} encode_json($stashedFolders);
        }
    }
    my @command = @{ $self->{parseCommand} }, @foldersToDo;
    warn "@command";
    system @command;
    delete @{$self}{@folders};
}

1;
