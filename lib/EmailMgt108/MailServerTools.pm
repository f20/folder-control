package EmailMgt108::MailServerTools;

# Copyright 2020-2023 Franck Latrémolière and others.
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
use utf8;

use Mail::IMAPClient;
use Time::Piece;
use File::Spec::Functions qw(catdir catfile);
use File::Basename qw(dirname);
use Digest::SHA;
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_INO STAT_MODE STAT_UID);
use YAML;

sub remove_or_append_messages {
    my ( $server, $account, $password, $mailboxUidToRemoveArrayref,
        $mailboxMessageDateToAppendArrayref,
    ) = @_;
    my $imap = Mail::IMAPClient->new(
        Ssl      => 1,
        Uid      => 1,
        Server   => $server,
        User     => $account,
        Password => $password,
    ) or die "connection error: $@\n";
    if ( 'ARRAY' eq ref $mailboxUidToRemoveArrayref ) {
        my $activeMailbox = '';
        foreach (@$mailboxUidToRemoveArrayref) {
            my ( $mailbox, $uid ) = @$_;
            $mailbox = "INBOX.$mailbox" unless $mailbox eq 'INBOX';
            if ( $mailbox ne $activeMailbox ) {
                $activeMailbox = $mailbox;
                $imap->select($mailbox) or die "select $mailbox error: $@\n";
            }
            $imap->move( 'INBOX.Trash', $uid )
              or die "move $mailbox $uid error: $@\n";
        }
        if ($activeMailbox) {
            $imap->close or die "close error: $@\n";
        }
    }
    if ( 'ARRAY' eq ref $mailboxMessageDateToAppendArrayref ) {
        foreach (@$mailboxMessageDateToAppendArrayref) {
            my ( $mailbox, $text, $date ) = @$_;
            $mailbox = "INBOX.$mailbox" unless $mailbox eq 'INBOX';
            $imap->append_string( $mailbox, $text, undef, $date )
              or die "append $mailbox $date error: $@\n";
        }
    }
}

# This is to work around the fact that running successive
# connections in the same process seems to go wrong.
sub email_downloader_forked {
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
    ) or die "$account connection error: $@\n";

    # No $imap->compress as that seems to break things.

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
        delete $folderHashref->{mailboxPath};
        delete $folderHashref->{mailArchivePath};
        $folderHashref = {} unless defined $folderHashref;
        $catalogueToStore{$folder} = $folderHashref;

        my ( $mailboxPath, $newSha1Hex ) = find_or_make_folder(
            $generalSettings{searchSha1},
            $folderHashref->{mailboxCaseidSha1Hex},
            %folderHashFromServer
            ? catdir( $mailboxesPath, "✉️$localName ($account)" )
            : undef
        );
        if ( defined $newSha1Hex
            and !defined $folderHashref->{mailboxCaseidSha1Hex}
            || $folderHashref->{mailboxCaseidSha1Hex} ne $newSha1Hex )
        {
            %$folderHashref = ( mailboxCaseidSha1Hex => $newSha1Hex );
        }
        next FOLDER unless defined $mailboxPath;
        $folderHashref->{mailboxPath} = $mailboxPath;

        my $mailArchivePath;
        if ( $mailArchivesPath && -d $mailArchivesPath ) {
            if (
                ( $mailArchivePath, my $newSha1Hex ) = find_or_make_folder(
                    $generalSettings{searchSha1},
                    $folderHashref->{mailArchiveCaseidSha1Hex},
                    catdir( $mailArchivesPath, "📎$localName ($account)" )
                )
              )
            {
                if ( !defined $folderHashref->{mailArchiveCaseidSha1Hex} ) {
                    $folderHashref->{mailArchiveCaseidSha1Hex} = $newSha1Hex;
                }
                elsif (
                    $folderHashref->{mailArchiveCaseidSha1Hex} ne $newSha1Hex )
                {
                    $folderHashref->{mailArchiveCaseidSha1Hex} = $newSha1Hex;
                    delete $_->{archived}
                      foreach grep { ref $_; } values %$folderHashref;
                }
                chdir $mailArchivePath;
                require EmailMgt108::EmailParser;
                $folderHashref->{mailArchivePath} = $mailArchivePath;
            }
        }

        {
            my $dh;
            opendir $dh, $mailboxPath;
            my $stashPath;
            while ( readdir $dh ) {
                next unless /^([0-9]+)\.eml$/s;
                next if exists $folderHashFromServer{$1};
                unless ( defined $stashPath ) {
                    $stashPath = catdir( $mailboxPath, 'Z_Removed' );
                    mkdir $stashPath;
                }
                rename catfile( $mailboxPath, $_ ), catfile( $stashPath, $_ );
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
            my $emlFile = catfile( $mailboxPath, "$uid.eml" );
            my @stat    = lstat $emlFile;
            if ( @stat && !$stat[7] ) {
                unlink $emlFile;
                @stat = ();
            }
            if (@stat) {
                warn join( ' ',
                    $mailboxPath, $uid, "diskBytes=$stat[7]",
                    "serverBytes=$folderHashFromServer{$uid}{'RFC822.SIZE'}" )
                  unless $stat[7] == $folderHashFromServer{$uid}{'RFC822.SIZE'};
            }
            elsif ( --$maxMessages ) {
                eval {
                    warn "$emlFile\n";
                    open my $mh, '>', $emlFile;
                    print {$mh} $imap->message_string($uid);
                };
                if ($@) {
                    warn "Message download error for $folder in $account\n";
                    last MESSAGE;
                }
            }
            else {
                warn "Messages not downloaded in $folder in $account\n";
                $maxMessages = 3;
                last MESSAGE;
            }
            my $lmod = Time::Piece->strptime(
                $folderHashFromServer{$uid}{'INTERNALDATE'},
                '%d-%b-%Y %H:%M:%S %z' )->epoch;
            utime time, $lmod, $emlFile if !@stat || $stat[9] > $lmod;
            $folderHashref->{$uid} =
              $mailArchivesPath
              ? {
                %{ $folderHashFromServer{$uid} },
                archived => $folderHashref->{$uid}
                  && $folderHashref->{$uid}{archived}
                  && -e $folderHashref->{$uid}{archived}
                ? $folderHashref->{$uid}{archived}
                : EmailMgt108::EmailParser::parseMessage($emlFile),
              }
              : $folderHashFromServer{$uid};
        }
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
            rename catdir( $mailboxesPath, "✉️$localName" ),
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
            rename $archivePath, catdir( $mailArchiveStashPath, "📎$localName" );
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
            next unless -s $path;
            $folder = dirname($path);
            last;
        }
    }
    return $folder, $caseidsha1hex if defined $folder;
    return unless defined $fallbackPath;
    my $caseidFile = catfile( $fallbackPath, '.caseid' );
    return $fallbackPath, $caseidsha1hex
      if -f $caseidFile
      && Digest::SHA->new->addfile( catfile( $fallbackPath, '.caseid' ) )
      ->hexdigest eq $caseidsha1hex;
    $fallbackPath .= '_' while -e $fallbackPath;
    mkdir $fallbackPath;
    return unless -d $fallbackPath;
    $caseidFile = catfile( $fallbackPath, '.caseid' );
    system 'dd', 'if=/dev/urandom', 'count=1', "of=$caseidFile";
    return $fallbackPath, Digest::SHA->new->addfile($caseidFile)->hexdigest;
}

1;
