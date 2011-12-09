use common::sense;
use Getopt::Long;
use AnyEvent::CouchDB;
use Coro;
use Coro::AnyEvent;

use constant CONCURRENCY => 10;
my $DB_PREFIX = 'ws228_';
sub until_not_timeout (&;$); # forward dec

my ($COMPACT_VIEWS, $COMPACT_DBS, $ALL);
GetOptions(
    'views'         => \$COMPACT_VIEWS,
    'databases|dbs' => \$COMPACT_DBS,
    'all'           => \$ALL,
);

my $couch = AnyEvent::CouchDB->new;
my @databases = grep /^${DB_PREFIX}/, @{ $couch->all_dbs->recv };

if ($ALL) {
    $COMPACT_VIEWS = $COMPACT_DBS = 1;
}

my $concurrency = Coro::Semaphore->new(CONCURRENCY);

my @coros;
push @coros, async { compact_views() },    if $COMPACT_VIEWS;
push @coros, async { compact_databases() } if $COMPACT_DBS;
$_->join foreach @coros;

sub compact_views {
    my @coros = map {
        my $db = $couch->db($_);
        my $class = dbn2class($_);
        async {
            my $guard = $concurrency->guard;
            until_not_timeout { $db->post("_compact/$class")->recv }
                              "$class view compaction";
        };
    } @databases;

    $_->join foreach @coros;
}

sub compact_databases {
    my @coros = map {
        my $db = $couch->db($_);
        my $class = dbn2class($_);
        async {
            my $guard = $concurrency->guard;
            until_not_timeout { $db->compact->recv } "$class db compaction";
        };
    } @databases;

    $_->join foreach @coros;
}

sub until_not_timeout (&;$) {
    my ($code, $id) = @_;

    my $wait_msg = 'waiting';
    $wait_msg .= " on $id" if length $id;

    while (!eval { $code->() }) {
        if ($@ !~ /time/i) {
            warn "Error when $wait_msg: $@\n";
            return;
        }
        say ucfirst $wait_msg;
        Coro::AnyEvent::sleep(10);
    }
    say "Success in $wait_msg";
    return 1;
}

sub dbn2class {
    (my $c = shift) =~ s/^${DB_PREFIX}//o;
    return $c;
}
