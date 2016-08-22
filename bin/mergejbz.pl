#!/usr/bin/env perl

=head Copyright licence and disclaimer

Copyright 2016 Franck Latrémolière.

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
use Carp;
$SIG{__DIE__} = \&Carp::confess;
binmode STDERR, ':utf8';
use Encode 'decode_utf8';

use File::Spec::Functions qw(catfile catdir rel2abs);
use File::Basename qw(dirname basename);
use Cwd;
my ( $startFolder, $perlFolder );

BEGIN {
    $SIG{INT} = $SIG{USR1} = $SIG{USR2} = sub {
        my ($sig) = @_;
        die "Died on $sig signal\n";
    };
    $startFolder = getcwd();
    my $homedir = dirname( rel2abs( -l $0 ? ( readlink $0, dirname $0) : $0 ) );
    while (1) {
        $perlFolder = catdir( $homedir, 'lib' );
        last if -d catdir( $perlFolder, 'FileMgt106' );
        my $parent = dirname $homedir;
        last if $parent eq $homedir;
        $homedir = $parent;
    }
    chdir $perlFolder or die "chdir $perlFolder: $!";
    $perlFolder = decode_utf8 getcwd();
    chdir $startFolder;
}
use lib $perlFolder;

my ( $localName,  $localTime,  $localScalar );
my ( $remoteName, $remoteTime, $remoteScalar );

{
    require FileMgt106::Tools;
    my ( $local, $remote ) = @ARGV;
    die "Usage:\n\tmerge.pl Local.jbz Remote.jbz [Folder]\n"
      unless defined $local
      and -f $local
      and $localTime   = ( stat _ )[9]
      and ($localName) = ( $local =~ /(\S+)\.jbz/ )
      and $localScalar = FileMgt106::Tools::loadJbz($local)
      and defined $remote
      and -f $remote
      and $remoteTime   = ( stat _ )[9]
      and ($remoteName) = ( $remote =~ /(\S+)\.jbz/ )
      and $remoteScalar = FileMgt106::Tools::loadJbz($remote);
}

my %byCore;

while ( my ( $key, $value ) = each %$localScalar ) {
    next unless ref $value eq 'HASH' && keys %$value;
    if ( $key =~ s/\s*\.mirror.*//si ) {
        next unless ref $value->{source} eq 'HASH';
        $byCore{$1}{ ( values %{ $value->{_SOURCE} } )[0] } = $value;
    }
    if ( $key =~ s/\s*(\+|\.addition).*//si ) {
        push @{ $byCore{$key}{additions} }, $value;
    }
    else {
        $byCore{$key}{master} = $value;
    }
}

while ( my ( $key, $value ) = each %$remoteScalar ) {
    next unless ref $value eq 'HASH' && keys %$value;
    if ( $key =~ s/\s*\.mirror.*//si ) {
        next unless ref $value->{source} eq 'HASH';
        $byCore{$1}{ ( values %{ $value->{_SOURCE} } )[0] } = $value;
    }
    if ( $key =~ s/\s*(\+|\.addition).*//si ) {
        push @{ $byCore{$key}{additions} }, $value;
    }
    else {
        $key .= " ($remoteName)" while $byCore{$key}{master};
        $byCore{$key}{$remoteTime} =
          { %$value, _SOURCE => { "$remoteName" => "$remoteTime", } };
    }
}

my $mergedScalar;
while ( my ( $key, $map ) = each %byCore ) {
    my $lead      = delete $map->{master};
    my $additions = delete $map->{additions};
    my $key2      = $key;
    unless ($lead) {
        if ( my ($topKey) = sort { $b <=> $a; } keys %$map ) {
            $lead = $map->{$topKey};
        }
        $lead ||= {};
        $key2 .= ' .mirrored';
    }
    if ($additions) {
        delete $lead->{_SOURCE};
        my $counter = '';
        foreach (@$additions) {
            while ( my ( $k, $v ) = each %$_ ) {
                my $c = '';
                --$c while _conflicts( $lead->{"$k$c"}, $v );
                $lead->{"$k$c"} = $v;
            }
            $mergedScalar->{"$key .additions$counter"} = $_;
            --$counter;
        }
    }
    $mergedScalar->{$key2} = $lead;
}

FileMgt106::Tools::saveJbzPretty( "$localName+$remoteName.jbz", $mergedScalar );

sub _conflicts {
    my ( $existing, $new ) = @_;
    return unless defined $existing && defined $new;
    my $filter = FileMgt106::Tools::makeInfillFilter();
    $filter->($existing);
    return $filter->($new);
}
