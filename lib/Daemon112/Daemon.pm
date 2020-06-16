package Daemon112::Daemon;

# Copyright 2008-2019 Franck Latrémolière, Reckon LLP and others and others.
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

=head Documentation

Daemon112::Daemon->run($module, $nickname, $logging, @args);

Things to do are either:
1. Scheduled in an ArrayQueue:
    closures, or objects capable of running the "dequeued" method.
2. Watched in a Daemon112::KQueue (Darwin/BSD only):
    closures, or objects capable of running the "kevented" method.

Both closures and methods receive $runner initialised as $module->new( $qu, $pq, $kq, @args).
For KQueue events, the kevent structure (array reference) is an extra parameter.

=cut

use strict;
use warnings;
use Daemon112::ArrayQueue;
use Encode qw(decode_utf8);

sub reloadMyModules {
    my $myInc = $INC{'Daemon112/Daemon.pm'};
    $myInc =~ s#Daemon112/Daemon\.pm$##s;
    my $l = length $myInc;
    foreach ( grep { substr( $INC{$_}, 0, $l ) eq $myInc; } keys %INC ) {
        warn "Force reload of $_";
        delete $INC{$_};
        require $_;
    }
}

sub run {
    my ( $self, $module, $nickName, $logging, @args ) = @_;

    # NB: "perl:" is needed to avoid confusing the rc.d script.
    $0 = 'perl: Daemon112 ' . ( $nickName ||= $module );

    if ( !( $logging ||= '' ) ) {
        require POSIX;
        $SIG{'__WARN__'} = sub {
            return if $_[0] =~ /^Subroutine .+ redefined at/;
            warn "[$$] " . POSIX::strftime( '%r: ', localtime ), @_;
        };
    }
    else {
        if ( $logging =~ /^syslog$/i ) {
            require Sys::Syslog;
            if ( -w '/var/run/daemon112-$nickName.pid' || -w '/var/run' ) {
                open my $pidfile, ">/var/run/daemon112-$nickName.pid";
                print $pidfile $$;
            }
            if ( -w '/var/log/daemon112-stderr' || -w '/var/log' ) {
                open STDERR, '>>', '/var/log/daemon112-stderr';
                open STDOUT, '>&STDERR';
            }
            Sys::Syslog::openlog( $nickName, "pid", "local5" );
            $SIG{'__WARN__'} = sub {
                return if $_[0] =~ /^Subroutine .+ redefined at/;
                Sys::Syslog::syslog( "info", join ' ', @_ );
            };
        }
        else {
            open STDERR, '>>', $logging or die $!;
            binmode STDERR, ':utf8';
            open STDOUT, '>&STDERR' or die $!;
            binmode STDOUT, ':utf8';
            my $nickPid = " $nickName\[$$]: ";
            require POSIX;
            $SIG{'__WARN__'} = sub {
                return if $_[0] =~ /^Subroutine .+ redefined at/;
                warn POSIX::strftime( '%b %e %H:%M:%S', localtime ) . $nickPid,
                  @_;
            };
        }
    }

    my $pq = new Daemon112::ArrayQueue;
    my $qu = new Daemon112::ArrayQueue;
    my $kq;
    $kq ||= new Daemon112::KQueue  if eval { require Daemon112::KQueue; };
    $kq ||= new Daemon112::Inotify if eval { require Daemon112::Inotify; };
    warn 'Install BSD::Resource and IO::KQueue, '
      . 'or Linux::Inotify2, to enable watching'
      unless $kq;

    my $runner;

    my %signalQueue = ( needsLoading => 1 );
    $SIG{HUP} = $SIG{TERM} = $SIG{INT} = $SIG{USR1} = $SIG{USR2} =
      sub { $signalQueue{ $_[0] } = 1; };

    while (1) {
        if (%signalQueue) {
            warn join ' ', 'Signals pending:', sort keys %signalQueue;
            if ( $signalQueue{HUP} ) {
                warn 'Reloading ' . __PACKAGE__ . ' with ' . $module;
                delete $signalQueue{HUP};
                reloadMyModules();
                $signalQueue{needsLoading} = 1;
            }
            if ( $signalQueue{TERM} || $signalQueue{INT} ) {
                warn 'Terminating ' . __PACKAGE__ . ' with ' . $module;
                require POSIX and POSIX::_exit(0);
                die 'This should not happen';
            }
            if ( $signalQueue{USR1} ) {
                warn 'Restarting ' . __PACKAGE__ . ' with ' . $module;
                require Data::Dumper;
                exec $^X, ( map { "-I$_"; } @INC ), '-M' . __PACKAGE__, '-e',
                    __PACKAGE__
                  . '->run(@{'
                  . Data::Dumper::Dumper(
                    [ $module, $nickName, $logging, @args ] )
                  . '})';
            }
            if ( $signalQueue{USR2} ) {
                warn 'Dumping ' . __PACKAGE__ . ' state with ' . $module;
                delete $signalQueue{USR2};
                warn 'Working directory: ' . decode_utf8(`pwd`);
                if ($pq) {
                    warn 'Priority queue: ' . ( 0 + @$pq ) . ' entries';
                }
                if ($qu) {
                    warn 'Standard queue: ' . ( 0 + @$qu ) . ' entries';
                }
                if ($kq) {
                    warn 'Watch queue: ' . ( 0 + keys %$kq ) . ' keys';
                }
                $runner->dumpState if UNIVERSAL::can( $runner, 'dumpState' );
            }
            if ( $signalQueue{needsLoading} ) {
                ++$signalQueue{needsLoading};
                eval "require $module;";
                if ($@) {
                    warn "Cannot load $module: $@";
                    reloadMyModules();
                    sleep $signalQueue{needsLoading};
                    next;
                }
                warn "Loaded $module";
                if ( !$runner ) {
                    eval { $runner = $module->new( $qu, $pq, $kq, @args ); };
                    if ( !$runner || $@ ) {
                        warn "Error or false result from $module->new: $@";
                        reloadMyModules();
                        sleep $signalQueue{needsLoading};
                        next;
                    }
                }

                eval { $runner->start; };
                if ($@) {
                    warn "Cannot start $module: $@";
                    reloadMyModules();
                    sleep $signalQueue{needsLoading};
                    next;
                }
                warn "Started $module";
                delete $signalQueue{needsLoading};
                next;
            }
        }
        my $time     = time;
        my $nextTime = $pq->get_next_priority();
        $nextTime = $nextTime ? $nextTime - $time : 60;
        if ( $nextTime > 0 ) {
            my $nextQu = $qu->get_next_priority();
            $nextQu = $nextQu ? $nextQu - $time : 60;
            if ( $nextQu > 0 ) {
                $nextTime = $nextQu if $nextQu < $nextTime;
            }
            else {
                $nextTime = 0;
                _runNextInQueue( $runner, $qu, $time );
            }
        }
        else {
            $nextTime = 0;
            _runNextInQueue( $runner, $pq, $time );
        }
        $nextTime = 60 if $nextTime > 60;
        unless ($kq) {
            sleep $nextTime;
            next;
        }
        eval {
            foreach ( $kq->events($nextTime) ) {
                my ( $obj, $e ) = @$_;
                next unless ref $obj;
                $time = time;
                ref $obj eq 'CODE'
                  ? $obj->( $runner, $e )
                  : $obj->kevented( $runner, $e );
                warn "Slow kevent ($time seconds) $obj"
                  if ( $time = time - $time ) > 4;
            }
        };
        warn $@ if $@;
    }
}

sub _runNextInQueue {
    my ( $runner,   $qu, $time ) = @_;
    my ( $priority, $id, $obj )  = $qu->dequeue_next();
    eval { ref $obj eq 'CODE' ? $obj->($runner) : $obj->dequeued($runner); };
    warn "{$obj} $@" if $@;
    warn "Slow ($time seconds) $obj"
      if ( $time = time - $time ) > 14;
}

1;
