use common::sense;
use Getopt::Long;
use AnyEvent::CouchDB;
use Coro;
use Coro::AnyEvent;

my ($COMPACT_VIEWS, $COMPACT_DBS, $ALL);
GetOptions(
    'views'         => \$COMPACT_VIEWS,
    'databases|dbs' => \$COMPACT_DBS,
    'all'           => \$ALL,
);

my $dbprefix = 'ws228_experimental';

my $couch = AnyEvent::CouchDB->new;
my @databases = grep /^_/ @{ $couch->all_dbs->recv };

if ($ALL) {
    $COMPACT_VIEWS = $COMPACT_DBS = 1;
}

my @coros;
push @coros, async { compact_views() }     if $COMPACT_VIEWS;
push @coros, async { compact_databases() } if $COMPACT_DBS;
$_->join foreach @coros;

sub compact_views {
    my @coros = map {
        my $db = $couch->db($_);
        (my $class = $_) =~ s/^${dbprefix}_//o;
        async {
            until_not_timeout { $db->post("_compact/$class")->recv }
                              "$class view compaction";
        };
    } @databases;

    $_->join foreach @coros;
}

sub compact_databases {
    my @coros = map {
        my $db = $couch->db($_);
        async {
            until_not_timeout { $db->compact->recv } "$class db compaction";
        };
    } @databases;

    $_->join foreach @coros;
}

sub until_not_timeout (&;$) {
    my ($code, $id) = @_;

    my $wait_msg = 'Waiting';
    $wait_msg .= " on $id" if length $id;

    while (!eval { $code->() }) {
        if ($@ !~ /timeout/i) {
            warn 'Error when ', lcfirst $wait_msg, ': ', $@;
            last;
        }
        warn $wait_msg, "\n";
        Coro::AnyEvent::sleep(10);
    }
}
