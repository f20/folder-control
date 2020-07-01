package EmailMgt108::MailServerTools;

# Copyright 2020 Franck Latrémolière and others.
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

use Mail::IMAPClient;
use Time::Piece;
use File::Spec::Functions qw(catdir catfile);
use File::Basename qw(dirname);
use Digest::SHA;
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_INO STAT_MODE STAT_UID);
use YAML;

sub remove_messages {
    my ( $server, $account, $password, $mailbox, @uidsToRemove ) = @_;
    my $imap = Mail::IMAPClient->new(
        Ssl      => 1,
        Uid      => 1,
        Server   => $server,
        User     => $account,
        Password => $password,
    ) or die "connection error: $@\n";
    $imap->select($mailbox) or die "select $mailbox error: $@\n";
    foreach (@uidsToRemove) {
        $imap->move( 'INBOX.Trash', $_ ) or die "move $_ error: $@\n";
    }
    $imap->close or die "close error: $@\n";
}

sub email_downloader_forked
{    # running successive connections in the same process seems to go wrong
    if ( my $pid = fork ) {
        waitpid $pid, 0;
    }
    else {
        email_downloader(@_);
        exit 0;
    }
}

sub email_downloader {

    my ( $server, $account, $password, $cataloguePath, $mailboxesPath,
        $mailArchivesPath, %generalSettings )
      = @_;

    my $storedCatalogue;
    $storedCatalogue = YAML::LoadFile($cataloguePath) if -s $cataloguePath;

    my %catalogueToStore;
    my $maxMessages = $generalSettings{maxMessages} || 0;

    my $imap = Mail::IMAPClient->new(
        Ssl      => 1,
        Uid      => 1,
        Peek     => 1,
        Server   => $server,
        User     => $account,
        Password => $password,
      )
      or die "$account connection error: $@\n"
      ;    # Do not do $imap->compress as it seems to break things
    my $folders = $imap->folders
      or die "$account list folders error: ", $imap->LastError, "\n";

  FOLDER: foreach my $folder (@$folders) {

        next if $folder eq 'INBOX.Trash';
        my $localName = $folder;
        $localName =~ s/^INBOX\.//s;

        $imap->select($folder) or next;
        my %folderHashFromServer =
          $imap->fetch_hash(qw(RFC822.SIZE INTERNALDATE FLAGS));
        my $folderHashref = delete $storedCatalogue->{$folder};

        ( my $mailboxPath, $folderHashref->{mailboxCaseidSha1Hex} ) =
          find_or_make_folder(
            $generalSettings{searchSha1},
            $folderHashref->{mailboxCaseidSha1Hex},
            catdir( $mailboxesPath, $localName )
          );

        my $mailArchivePath;
        if ( $mailArchivesPath && -d $mailArchivesPath ) {
            ( $mailArchivePath, $folderHashref->{mailArchiveCaseidSha1Hex} ) =
              find_or_make_folder(
                $generalSettings{searchSha1},
                $folderHashref->{mailArchiveCaseidSha1Hex},
                catdir( $mailArchivesPath, $localName )
              );
            chdir $mailArchivePath;
            require EmailMgt108::EmailParser;
        }

        {
            my $dh;
            opendir $dh, $mailboxPath;
            my $stashPath;
            while ( readdir $dh ) {
                next unless /^([0-9]+).$/s;
                next if exists $folderHashFromServer{$1};
                unless ( defined $stashPath ) {
                    $stashPath = catdir( $mailboxPath, 'Z_Removed' );
                    mkdir $stashPath;
                }
                rename catfile( $mailboxPath, "$1." ),
                  catfile( $stashPath, "$1." );
                if ( defined $mailArchivesPath
                    && ( my $archived = $folderHashref->{$1}{archived} ) )
                {
                    local $_ = $archived;
                    s%Y_([^/]+)$%Z_$1%s;
                    rename $archived, $_;
                }
            }
        }

      MESSAGE: foreach my $uid ( keys %folderHashFromServer ) {
            my $tfile = catfile( $mailboxPath, "$uid." );
            my @stat = lstat $tfile;
            if ( @stat && !$stat[7] ) {
                unlink $tfile;
                @stat = ();
            }
            if (@stat) {
                warn join( ' ',
                    $mailboxPath, $uid, "diskBytes=$stat[7]",
                    "serverBytes=$folderHashFromServer{$uid}{'RFC822.SIZE'}" )
                  unless $stat[7] == $folderHashFromServer{$uid}{'RFC822.SIZE'};
            }
            elsif ( --$maxMessages ) {
                open my $mh, '>', $tfile;
                print {$mh} $imap->message_string($uid);
            }
            else {
                warn "Stopping $account\n";
                last FOLDER;
            }
            my $lmod = Time::Piece->strptime(
                $folderHashFromServer{$uid}{'INTERNALDATE'},
                '%d-%b-%Y %H:%M:%S %z' )->epoch;
            utime time, $lmod, $tfile if !@stat || $stat[9] > $lmod;
            $folderHashref->{$uid} =
              $mailArchivesPath
              ? {
                %{ $folderHashFromServer{$uid} },
                archived => $folderHashref->{$uid}
                  && $folderHashref->{$uid}{archived}
                ? $folderHashref->{$uid}{archived}
                : EmailMgt108::EmailParser::parseMessage($tfile),
              }
              : $folderHashFromServer{$uid};
        }
        $catalogueToStore{$folder} = $folderHashref;
    }

    {
        my ( $mailboxStashPath, $mailArchiveStashPath );
        foreach my $deletedFolder ( keys %$storedCatalogue ) {
            my $localName = $deletedFolder;
            $localName =~ s/^INBOX\.//s;
            unless ( defined $mailboxStashPath ) {
                $mailboxStashPath = catdir( $mailboxesPath, 'Z_Removed' );
                mkdir $mailboxStashPath;
            }
            rename catdir( $mailboxesPath, $localName ),
              catdir( $mailboxStashPath, $localName );
            next unless $mailArchivesPath;
            my $archivePath = catdir( $mailArchivesPath, $localName );
            next
              unless -e $archivePath;
            unless ( defined $mailArchiveStashPath ) {
                $mailArchiveStashPath =
                  catdir( $mailArchivesPath, 'Z_Removed' );
                mkdir $mailArchiveStashPath;
            }
            rename $archivePath, catdir( $mailArchiveStashPath, $localName );
        }
    }

    unlink $cataloguePath;
    YAML::DumpFile( $cataloguePath, \%catalogueToStore );
    $imap->disconnect or warn "disconnect error: $@\n";

}

sub find_or_make_folder {
    my ( $searchSha1, $caseidsha1hex, $fallbackPath, ) = @_;
    my $folder;
    if ( $searchSha1 && defined $caseidsha1hex ) {
        my $iterator = $searchSha1->( pack( 'H*', $caseidsha1hex ) );
        while ( my ($path) = $iterator->() ) {
            next if $path =~ m#/Y_Cellar.*/#;
            $folder = dirname($path);
            last;
        }
    }
    if ( defined $folder ) {
        if ( defined $fallbackPath ) {
            unlink $fallbackPath;
            symlink $folder, $fallbackPath;
        }
        return $folder, $caseidsha1hex;
    }
    return              unless defined $fallbackPath;
    mkdir $fallbackPath unless -e $fallbackPath;
    return              unless -d $fallbackPath;
    my $caseidFile = catfile( $fallbackPath, '.caseid' );
    system 'dd', 'if=/dev/urandom', 'count=1', "of=$caseidFile"
      unless -e $caseidFile;
    return $fallbackPath, Digest::SHA->new->addfile($caseidFile)->hexdigest;
}

1;
