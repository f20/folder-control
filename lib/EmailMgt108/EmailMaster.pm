package EmailMgt108::EmailMaster;

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

use strict;
use warnings;
use JSON;

sub new {
    my ( $class, $runner ) = @_;
    bless { queue => $runner->{pq}, }, $class;
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

    my @todo;
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
        my $folders = {};
        my $toScan  = {};
        local undef $/;
        if ( open my $h, '<', '~$ email index $~/unstashed.json' ) {
            binmode $h;
            $folders = decode_json(<$h>) || {};
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
                return if $folders->{$scalar};
                $toScan->{$scalar} = $location->();
            };
            $run->( $scalar, sub { $imapRepo; } );
        };
        while ( my ( $imapRepo, $scalar ) = each %{ $self->{$_} } ) {
            $addScalarToMap->( $imapRepo, $scalar );
        }
        my $keep = $dbh->prepare('select sha1hex from map group by sha1hex');
        $keep->execute;
        my %toDelete = %$folders;
        while ( my ($sha1hex) = $keep->fetchrow_array ) {
            delete $toDelete{$sha1hex};
        }
        my $status;
        sleep 2 while !( $status = $dbh->commit );
        $dbh->disconnect;
        while ( my ( $sha1hex, $folder ) = each %toDelete ) {
            if ( defined $folder ) {
                my $folder2 = $folder;
                $folder2 =~ s#^(.*)/##s;
                $folder2 =~ s#^Y?_?#Z_#s;
                rename $folder, $folder2 or next;
                delete $folders->{$sha1hex};
                rmdir $1 if defined $1;
            }
        }
        if ( keys %$toScan ) {
            push @todo, [ $_, $toScan, $folders ];
        }
        elsif ( open my $h, '>', '~$ email index $~/unstashed.json' ) {
            binmode $h;
            print {$h} encode_json($folders);
        }
    }
    return unless @todo;

    my $pid = fork;
    unless ( defined $pid ) {
        warn "Cannot fork for email parsing: $!";
        return;
    }
    if ($pid) {
        warn "Forked $pid for email parsing";
        delete $self->{ $_->[0] } foreach @todo;
        $self->{pid} = $pid;
        $self->schedule;
        return;
    }

    eval {

        foreach (@todo) {
            my $h;
            opendir $h, $_->[0];
            $_->[0] = $h;
        }

        require POSIX;
        POSIX::setgid(6);
        POSIX::setuid(60);
        POSIX::setsid();

        require EmailMgt108::EmailParser;

        foreach (@todo) {
            my ( $dirHandle, $toScan, $folders ) = @$_;
            chdir $dirHandle or next;
            open my $scanningHandle, '>', '~$ email index $~/scanning' or next;
            while ( my ( $sha1hex, $emailFile ) = each %$toScan ) {
                eval {
                    $folders->{$sha1hex} =
                      EmailMgt108::EmailParser::parseMessage($emailFile);
                };
                warn "$emailFile: $@" if $@;
            }
            binmode $scanningHandle;
            print {$scanningHandle} encode_json($folders);
            close $scanningHandle;
            rename '~$ email index $~/scanning',
              '~$ email index $~/unstashed.json';
            unlink '~$ email index $~/toscan.json';
        }

    };
    warn $@ if $@;
    require POSIX and POSIX::_exit(0);
    die 'This should not happen';

}

1;
