package Daemon112::KQWeb;

# Copyright 2012 Franck Latrémolière, Reckon LLP and others.
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
use HTTP::Daemon;
use HTTP::Response;
use Daemon112::KQueue qw(:constants);
use Fcntl;

sub new {
    my ( $class, $options, $kqueue ) = @_;
    my $daemon = HTTP::Daemon->new(%$options) or do { warn $!; return; };
    fcntl( $daemon, F_SETFL, O_NONBLOCK );
    my $self = bless { daemon => $daemon }, $class;
    $kqueue->EV_SET( $daemon->fileno, EVFILT_READ, EV_ADD | EV_CLEAR,
        0, 0, $self );
    warn $class . ' started at ' . $daemon->url . "\n";
    $self;
}

sub kevented {
    my ( $self, $runner, $kevent ) = @_;
    while ( my $connection = $self->{daemon}->accept ) {
        fcntl( $connection, F_SETFL, O_NONBLOCK );
        my $action;
        my $complete = sub {
            my ($runner) = @_;
            $connection->close;
            $runner->{kq}->delete_objects( sub { $_[0] == $action; } );
        };
        $action = sub {
            my ( $runner, $kevent ) = @_;
            while ( my $request = $connection->get_request ) {
                $self->reply( $connection, $request );
            }
            $complete->($runner);
        };
        $runner->{kq}
          ->EV_SET( $connection->fileno, EVFILT_READ, EV_ADD | EV_CLEAR,
            0, 0, $action );
        $runner->{qu}->enqueue( time + 3, $complete );
    }
}

sub setHandler {
    my ( $self, $method, $path, $handler ) = @_;
    $self->{$method}{$path} = $handler;
    $self;
}

sub setHandlers {
    my ( $self, $path, $moduleOrObject ) = @_;
    $self->{$path} = $moduleOrObject;
    $self;
}

sub reply {
    my ( $self, $connection, $request ) = @_;
    my $method = $request->method;
    my $uri    = $request->uri;
    my $path   = $uri->path;
    if ( my $handler = $self->{$method}{$path} ) {
        return $handler->( $connection, $request );
    }
    elsif ( my $module = $self->{$path} ) {
        return $module->handler( $connection, $request );
    }
    my $message = "No handler for $method $path";
    $message .= "\nYour query was " . $uri->query
      if defined $uri->query;
    $message =~ s/&/&amp;/g;
    $message =~ s/</&lt;/g;
    my $content =
        '<!DOCTYPE html><html><head><title>Not found</title></head>'
      . '<body><h1>Not found</h1><pre>'
      . $message
      . '</pre></body></html>';
    $connection->send_response(
        HTTP::Response->new(
            404,
            'Not Found',
            [ 'Content-type', 'text/html', 'Content-length', length($content) ],
            $content
        )
    );
}

1;
