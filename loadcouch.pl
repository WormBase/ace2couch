#!/usr/bin/perl
# loadcouch.pl

# Jace -> Couch structured objects
# provide the jace as files or through STDIN

use common::sense;
use Getopt::Long;
use URI::Escape::XS qw(uri_escape);
use WormBase::JaceConverter qw(treematrix2hash);
use AD::Couch; # buffered couchloader, unfortunate namespace. WIP
use Time::HiRes qw(time);
use Ace; # for split
use Parallel::ForkManager;

use constant LOCALHOST          => '127.0.0.1';
use constant MEMORY_REQUIREMENT => 10_000_000; # 10 MB in theory

my ($host, $port, $db) = (LOCALHOST, 5984, 'test');
my $refresh_views = 1;
my $quiet;

GetOptions(
    'host=s'        => \$host,
    'port=s'        => \$port,
    'database|db=s' => \$db,
    'quiet'         => \$quiet,
    'refresh-views' => \$refresh_views,
);

unless ($quiet) {
    print "Will load to http://$host:$port/$db\n";
    print 'Is that okay? ';
    my $res = <STDIN>;
    exit unless $res =~ /^y/i;
}

my ($total_time, $total_count) = (0,0);

my $pm = Parallel::ForkManager->new(1); # memory management
$pm->run_on_finish(
    sub {
        my ($pid, $exit_code, undef, undef, undef, $data) = @_;
        $total_count += $data->[0];
        $total_time  += $data->[1];
        print $total_count, ' in ', $total_time," s\n";
        print $total_count/$total_time, '/s ', $total_time/$total_count, " s (avg)\n";
    }
);

MAIN:
while () {
    ## read in just enough data and then fork a worker to do the job
    my $buffer = '';
    READ: {
        local $/ = "\n\n";
        while (length $buffer < MEMORY_REQUIREMENT) {
            my $datum = <>;
            last MAIN unless length $buffer || defined $datum;
            last READ unless defined $datum;
            $buffer .= $datum;
        }
    }

    $pm->start and next MAIN;

    my $couch = AD::Couch->new(
        host                   => $host,
        port                   => $port,
        database               => $db,
        max_buffer_size        => MEMORY_REQUIREMENT,
                                  # probably won't be reached half the time
        refresh_views_on_flush => $refresh_views,
    );

    my ($time, $count) = (0,0);
    open my $bufferfh, '<', \$buffer;

  WORK:
    while () {
        my ($t1, $t2);
        $t1 = time;

        my ($data, $data_size);
        {
            local $/ = "\n\n";
            $data = <$bufferfh>;
            last WORK unless defined $data;
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
        $time += $t2 - $t1;

    }

    use constant SUCCESS_EXIT_CODE => 0;
    $pm->finish(SUCCESS_EXIT_CODE, [ $count, $time ]);
}

$pm->wait_all_children;
