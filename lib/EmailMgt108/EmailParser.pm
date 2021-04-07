package EmailMgt108::EmailParser;

# Copyright 2012-2021 Franck Latrémolière and others.
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

# To test from the command line on an email file called 1.:
# perl -I/path/to/lib -MEmailMgt108::EmailParser -e 'EmailMgt108::EmailParser::parseMessage("1.")'

use strict;
use warnings;
use Email::MIME;
use File::Spec::Functions qw(catdir catfile);
use POSIX;
use Unicode::Normalize qw(NFKD);

BEGIN {
    die 'Do not use EmailParser as root' unless $>;
}

use constant STAT_MTIME => 9;

sub getRawBody {
    my ($item) = @_;
    my $body = eval { $item->body_raw };
    warn "$@ for $item body_raw" if $@;
    my $how = eval { $item->header("Content-Transfer-Encoding"); }
      || '';
    warn "$@ for $item transfer encoding" if $@;
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
        warn "$@ for decode_$how" if $@;
    }

    $body;
}

sub parseMessage {
    my ( $emailFile, $container, $destinationFolder ) = @_;
    my $mtime = ( stat $emailFile )[STAT_MTIME];
    unless ( defined $mtime ) {
        warn "Cannot stat $emailFile";
        return;
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
    unless ( defined $destinationFolder ) {
        my @localtime = localtime $mtime;
        $destinationFolder = POSIX::strftime( '%Y-%m', @localtime );
        $destinationFolder = catdir( $container, $destinationFolder )
          if defined $container;
        mkdir $destinationFolder unless -e $destinationFolder;
        $destinationFolder = catdir( $destinationFolder,
            'Y_' . POSIX::strftime( '%Y-%m-%d %H-%M-%S', @localtime ) );
        $destinationFolder .= " ($1)" if $emailFile =~ m/([0-9]+)\.$/s;

        {
            my @subjectAndSender = (
                $email->header('Subject') || 'No identification',
                $email->header('From') || 'No sender'
            );
            eval { $_ = NFKD($_); } foreach @subjectAndSender;
            $subjectAndSender[1] =~ s/.* //;
            do {
                s/\pM//g;
                s/[^a-zA-Z0-9.,@()-]+/ /gs;
                s/^ //;
                s/ $//;
              }
              foreach @subjectAndSender;
            $subjectAndSender[0] =~ s/^(.{25,70}\S) .*?$/$1/s
              if length( $subjectAndSender[0] ) > 70;
            my $identification = "@subjectAndSender";
            $identification = substr( $identification, 0, 100 )
              if length($identification) > 100;
            $identification =~ s/[. ]+$//;
            $destinationFolder .= " $identification";
        }

        if (   -e $destinationFolder
            || -e "$destinationFolder.tmp" )
        {
            my $counter = -2;
            --$counter
              while -e "$destinationFolder$counter"
              || -e "$destinationFolder$counter.tmp";
            $destinationFolder .= $counter;
        }
    }
    unless ( mkdir "$destinationFolder.tmp" ) {
        warn "Cannot make $destinationFolder.tmp for $emailFile: $!";
        return;
    }
    my $savewarn = $SIG{__WARN__};
    my @warnings;
    $SIG{__WARN__} = sub { push @warnings, @_; };
    eval {
        my $fn0   = 'Email.txt';
        my @files = ($fn0);
        open my $fh, '>', "$destinationFolder.tmp/$fn0" or die $!;
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
            my $fn = $item->filename;
            $fn =~ tr/\000-\037\/\\/ / if $fn;
            my $encodingLayer = ':raw';
            if ( !$fn || -e $fn || $fn =~ /^=/s ) {
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
                if ( !$ct ) { }
                elsif ( $ct =~ m#message/rfc822#i ) {
                    $ext = '.eml';
                }
                elsif ( $ct =~ m#text/html#i ) {
                    $ext = '.html';
                }
                elsif ( $ct =~ m#image/(gif|jpe?g|png)#i ) {
                    $ext = ".$1";
                }
                elsif ( $ct =~ m#text/calendar#i ) {
                    $ext = '.ics';
                }
                elsif ( $ct =~ m#application/pdf#i ) {
                    $ext = '.pdf';
                }
                elsif ( $ct =~
m#application/vnd\.openxmlformats-officedocument\.wordprocessingml\.document#i
                  )
                {
                    $ext = '.docx';
                }
                elsif ( $ct =~
m#application/vnd\.openxmlformats-officedocument\.presentationml\.presentation#i
                  )
                {
                    $ext = '.pptx';
                }
                elsif ( $ct =~
m#application/vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet#i
                  )
                {
                    $ext = '.xlsx';
                }

                else {
                    eval {
                        open my $fh2, ">",
                          "$destinationFolder.tmp/$fn headers.txt"
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
                    _unzipfolder("$destinationFolder.tmp/winmail");
                    return;
                }
                elsif ( defined $pid ) {
                    close $w;
                    open \*STDIN, '<&', fileno($r);
                    mkdir "$destinationFolder.tmp/winmail"
                      and chdir "$destinationFolder.tmp/winmail"
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
                open my $fh3, '>', "$destinationFolder.tmp/$fn" or die $!;
                binmode $fh3, ':raw' or die $!;
                print {$fh3} getRawBody($item) or die $!;
                close $fh3 or die $!;
                if ( $fn =~ /(.*)\.eml$/is ) {
                    if (
                        parseMessage(
                            "$destinationFolder.tmp/$fn", undef,
                            "$destinationFolder.tmp/$1"
                        )
                      )
                    {
                        mkdir "$destinationFolder.tmp/Z_Unpacked";
                        rename "$destinationFolder.tmp/$fn",
                          "$destinationFolder.tmp/Z_Unpacked/$fn";
                    }
                }
                if ( $fn =~ /(.*)\.zip$/is ) {
                    _unzipfile( "$destinationFolder.tmp", $fn, $1 );
                }
            };
            warn "$fn: $@" if $@;
        };
        $email->walk_parts($partEater);
        close $fh or warn $!;
        utime time, $mtime, map { "$destinationFolder.tmp/$_" } @files
          or warn $!;
    };
    if ($@) {
        my $message = "$emailFile -> $destinationFolder:\n$@";
        if ( open my $h, '>', "$destinationFolder.tmp/Email parsing error.txt" )
        {
            binmode $h, ':utf8';
            print {$h} $message;
        }
    }
    $SIG{__WARN__} = $savewarn;
    if (@warnings) {
        open my $fh, '>', "$destinationFolder.tmp/Parser warnings.txt";
        print {$fh} join "\n",
          map { local $_ = $_; s# at /.*##; $_; } @warnings;
        close $fh;
    }
    rename "$destinationFolder.tmp", $destinationFolder
      or warn "rename $destinationFolder.tmp: $!";
    $destinationFolder;
}

sub _unzipfile {
    my ( $container, $zipfile, $destinationFolder ) = @_;
    my $pid = fork;
    if ($pid) {
        my $result = waitpid $pid, 0;
        unless ( $? >> 8 ) {
            mkdir "$container/Z_Unpacked";
            rename "$container/$zipfile", "$container/Z_Unpacked/$zipfile";
        }
        _unzipfolder("$container/$destinationFolder");
    }
    elsif ( defined $pid ) {
        exec qw(unzip -q -n -d), $destinationFolder, $zipfile
          if chdir $container;
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

