package EmailMgt108::EmailParser;

=head Copyright licence and disclaimer

Copyright 2012-2016 Franck Latrémolière, Reckon LLP.

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

# To test:
# perl -I/path/to/perl5/folder -MEmailMgt108::EmailParser -e 'EmailMgt108::EmailParser::parseMessage($_) foreach "1."'

use strict;
use warnings;
use utf8;

BEGIN {
    die 'Do not use EmailParser as root' unless $>;
}

use Email::MIME;
use POSIX;
use Unicode::Normalize qw(NFKD);
use constant STAT_MTIME => 9;

sub getRawBody {
    my ($item) = @_;
    my $body = eval { $item->body_raw };
    warn "$@ in " . `pwd` . " for $item body_raw" if $@;
    my $how = eval { $item->header("Content-Transfer-Encoding"); }
      || '';
    warn "$@ in " . `pwd` . " for $item transfer encoding" if $@;
    $how =~ s/\A\s+//;
    $how =~ s/\s+\z//;
    $how =~ s/;.+//;    # For S/MIME, etc
    $how = lc $how;
    $how = "qp"
      if $how eq "quotedprint"
      || $how eq "quoted-printable";

    if ( my $sub = Email::MIME::Encodings->can("decode_$how") ) {
        eval {
            $body = $sub->($body);
            use bytes;
            $body =~ s/\n\n$/\n/s
              if $how eq 'qp';
        };
        warn "$@ in " . `pwd` . " for decode_$how" if $@;
    }

    $body;
}

sub parseMessage {
    my ( $emailFile, $destination ) = @_;
    my $mtime = ( stat $emailFile )[STAT_MTIME];
    unless ( defined $mtime ) {
        warn "Cannot stat $emailFile";
        return;
    }
    my $folder;
    unless ($destination) {
        my @localtime = localtime $mtime;
        $folder = POSIX::strftime( '%Y-%m', @localtime );
        mkdir $folder unless -e $folder;
        $folder .= '/Y_' . POSIX::strftime( '%Y-%m-%d %H-%M-%S', @localtime );
    }
    open my $mfh, '<', $emailFile or die "Cannot open $emailFile: $!";
    binmode $mfh, ':raw';
    local undef $/;
    my $email = new Email::MIME(<$mfh>);
    unless ($email) {
        warn "Cannot parse $emailFile";
        return;
    }
    close $mfh or warn $!;
    if ($destination) {
        $folder = $destination;
    }
    else {
        my @titles = (
            $email->header('Subject') || 'No subject',
            $email->header('From') || 'No sender'
        );
        eval { $_ = NFKD($_); } foreach @titles;
        $titles[1] =~ s/.* //;
        do {
            s/\pM//g;
            s/[^a-zA-Z0-9.,@()-]+/ /gs;
            s/^ //;
            s/ $//;
          }
          foreach @titles;
        $titles[0] =~ s/^(.{25,70}\S) .*?$/$1/s
          if length( $titles[0] ) > 70;
        my $subject = "@titles";
        $subject = substr( $subject, 0, 100 ) if length($subject) > 100;
        $subject =~ s/[. ]+$//;
        $folder .= " $subject";
        if ( -e $folder || -e "$folder.tmp" ) {
            my $counter = -2;
            --$counter while -e $folder . $counter || -e "$folder$counter.tmp";
            $folder .= $counter;
        }
    }
    unless ( mkdir "$folder.tmp" ) {
        warn "Cannot make $folder.tmp for $emailFile: $!";
        return;
    }
    my $savewarn = $SIG{__WARN__};
    my @warnings;
    $SIG{__WARN__} = sub { push @warnings, @_; };
    eval {
        my $fn0   = 'Email.txt';
        my @files = ($fn0);
        open my $fh, '>', "$folder.tmp/$fn0" or die $!;
        binmode $fh, ':utf8' or die $!;
        print {$fh} join "\r\n", '', (
            map {
                my $h = $_;
                map { "$h: $_"; } eval { $email->header($_) } || $@;
            } $email->headers
          ),
          '';
        my $partEater;
        my %eaten;    # to avoid a weird infinite loop on some messages
        $partEater = sub {
            my ($item) = @_;
            return if $eaten{ 0 + $item } || $item->subparts;
            $eaten{ 0 + $item } = 1;
            my $fn            = $item->filename;
            my $encodingLayer = ':raw';
            if (  !$fn
                || $fn !~ /^[a-zA-Z\&0-9_ \.,;\+\-'"\(\)\[\]]+$/s
                || -e $fn )
            {
                my $ext = '.dat';
                $ext = $1
                  if $fn && $fn =~ /(\.[a-zA-Z0-9\+\-_]+)$/s;
                my $ct = $item->header('content-type');
                if ( $ct && $ct =~ m#\btext/plain#i ) {
                    eval { print {$fh} "\r\n", $item->body_str, "\r\n"; };
                    warn $@ if $@;
                    return;
                }
                $fn = 'Item ' . @files;
                if ( $ct && $ct =~ m#message/rfc822#i ) {
                    $ext = '.eml';
                }
                elsif ( $ct && $ct =~ m#text/html#i ) {
                    $ext = '.html';
                }
                elsif ( $ct && $ct =~ m#image/(gif|jpe?g|png)#i ) {
                    $ext = ".$1";
                }
                else {
                    eval {
                        open my $fh2, ">", "$folder.tmp/$fn headers.txt"
                          or die $!;
                        binmode $fh2, ':utf8' or die $!;
                        print {$fh2} join "\r\n", map {
                            my $h = $_;
                            map { "$h: $_"; } $item->header($_);
                        } $item->headers;
                        close $fh2 or die $!;
                    };
                    warn "$fn: $@" if $@;
                }
                $fn .= $ext;
            }
            if ( $fn eq 'winmail.dat' ) {
                pipe my $r, my $w;
                my $pid = fork;
                if ($pid) {
                    close $r;
                    print {$w} getRawBody($item);
                    close $w;
                    waitpid $pid, 0;
                    _unzipfolder("$folder.tmp/winmail");
                    return;
                }
                elsif ( defined $pid ) {
                    close $w;
                    open \*STDIN, '<&', fileno($r);
                    mkdir "$folder.tmp/winmail"
                      and chdir "$folder.tmp/winmail"
                      and exec qw(tnef --save-body);
                    local undef $/;
                    <$r>;
                    close $r;
                    require POSIX and POSIX::_exit(0);
                    die 'This should not happen';
                }
            }
            unshift @files, $fn;
            eval {
                open my $fh3, '>', "$folder.tmp/$fn" or die $!;
                binmode $fh3, ':raw' or die $!;
                print {$fh3} getRawBody($item) or die $!;
                close $fh3 or die $!;
                if ( $fn =~ /(.*)\.eml$/is ) {
                    if ( parseMessage( "$folder.tmp/$fn", "$folder.tmp/$1" ) ) {
                        mkdir "$folder.tmp/Z_Unpacked";
                        rename "$folder.tmp/$fn", "$folder.tmp/Z_Unpacked/$fn";
                    }
                }
                if ( $fn =~ /(.*)\.zip$/is ) {
                    _unzipfile( "$folder.tmp", $fn, $1 );
                }
            };
            warn "$fn: $@" if $@;
        };
        $email->walk_parts($partEater);
        close $fh or warn $!;
        utime time, $mtime, map { "$folder.tmp/$_" } @files or warn $!;
    };
    if ($@) {
        my $message = "$emailFile -> $folder:\n$@";
        if ( open my $h, '>', "$folder.tmp/Email parsing error.txt" ) {
            binmode $h, ':utf8';
            print {$h} $message;
        }
    }
    $SIG{__WARN__} = $savewarn;
    if (@warnings) {
        open my $fh, '>', "$folder.tmp/Parser warnings.txt";
        print {$fh} join "\r\n\r\n", "Warnings in parsing $emailFile",
          @warnings;
        close $fh;
    }
    rename "$folder.tmp", $folder or warn "rename $folder.tmp: $!";
    $folder;
}

sub _unzipfile {
    my ( $container, $zipfile, $folder ) = @_;
    my $pid = fork;
    if ($pid) {
        my $result = waitpid $pid, 0;
        unless ( $? >> 8 ) {
            mkdir "$container/Z_Unpacked";
            rename "$container/$zipfile", "$container/Z_Unpacked/$zipfile";
        }
        _unzipfolder("$container/$folder");
    }
    elsif ( defined $pid ) {
        exec qw(unzip -q -n -d), $folder, $zipfile if chdir $container;
        require POSIX and POSIX::_exit(0);
        die 'This should not happen';
    }
}

sub _unzipfolder {
    my ($container) = @_;
    my $dh;
    opendir $dh, $container or return;
    foreach ( readdir $dh ) {
        next if /^\.\.?$/s;
        if ( -d "$container/$_" ) {
            _unzipfolder("$container/$_");
        }
        elsif (/(.*)\.zip$/is) {
            _unzipfile( $container, $_, $1 );
        }
    }
}

1;

