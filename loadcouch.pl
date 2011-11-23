#!/usr/bin/perl
# loadcouch.pl

# Jace -> Couch structured objects
# provide the jace as files or through STDIN

use strict;
use warnings;
use Getopt::Long;
use URI::Escape::XS qw(uri_escape);
use WormBase::JaceConverter qw(treematrix2hash);
use AD::Couch; # buffered couchloader, unfortunate namespace. WIP
use Time::HiRes qw(time);
use Ace; # for split

use constant LOCALHOST => '127.0.0.1';

my ($host, $port, $db) = (LOCALHOST, 5984, 'test');
my $quiet;

GetOptions(
    'host=s'        => \$host,
    'port=s'        => \$port,
    'database|db=s' => \$db,
    'quiet'         => \$quiet,
);

unless ($quiet) {
    print "Will load to http://$host:$port/$db\n";
    print 'Is that okay? ';
    my $res = <STDIN>;
    exit unless $res =~ /^y/i;
}

my $couch = AD::Couch->new(
    host      => $host,
    port      => $port,
    database  => $db,
    blocksize => 1_000_000, # memory requirement
    nocheck   => 1,         # don't fetch revs, just dump
);

$SIG{INT} = $SIG{TERM} = $SIG{QUIT} = sub {
    undef $couch; # cleanup
    exit 0;
};

my $count = 0;
my $total_time = 0;

LOOP:
while () {
    my ($t1, $t2);
    $t1 = time;

    my ($data, $data_size);
    {
        local $/ = "\n\n";
        $data = <>;
        last LOOP unless defined $data;
    }

    $data_size = length $data;
    ## parse input into matrix
    open my $table, '<', \$data;

    my ($treewidth, $matrix) = (0, []);
    while (<$table>) {
        chomp;
        my @row = split /\t/;
        push @$matrix, \@row;
        $treewidth = @row if @row > $treewidth;
    }
    # free up memory
    undef $data;
    undef $table;

    ## parse matix into hash structure
    my $hash = treematrix2hash($matrix, 0, 0, undef, $treewidth);
    unless ($hash) {
        warn 'Could not parse data into a hash structure';
        next;
    }
    my ($class, $name) = Ace->split($matrix->[0][0]);
    undef $matrix;

    ## rearrange the hash into Couch doc format (with _id)
    my $key = (keys %$hash)[0];
    $hash = $hash->{$key};
    $hash->{_id} = uri_escape($key);
    @{$hash}{'class','name'} = ($class, $name);

    ## load into Couch
    $couch->add_doc($hash, $data_size); # will flush periodically to couch

    $t2 = time;
    ++$count;
    $total_time += $t2 - $t1;

    if ($count % 1000 == 0) {
        local $| = 1;
        print $count, ' in ', $total_time," s\n";
        print $count/$total_time, '/s ', $total_time/$count, " s (avg)\n";
    }
}
