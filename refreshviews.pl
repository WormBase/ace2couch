use common::sense;
use Coro;
use Coro::AnyEvent;
use AnyEvent;
use AnyEvent::CouchDB;

my $dbprefix = shift // 'ws228_';

my $couch = AnyEvent::CouchDB->new; # localhost 5984

my $concurrency = Coro::Semaphore->new(4); # only allow 4 to work at once

my @coros;
@coros = map {
    my $db = $couch->db($_);

    async {
        for my $ddoc ( map { $_->{doc} } @{ get_all_ddocs($db)->recv->{rows} } ) {
            my $ddn = $db->name . '/' . $ddoc->{_id};
            push @coros, async {
                my $guard = $concurrency->guard;
                print "Refreshing $ddn\n";

                (my $model = $ddoc->{_id}) =~ s{_design/}{};

                # try getting the view until it's generated
                my $result;
                while (!($result = eval { $db->view($model . '/id', { limit => 1 } )->recv })) {
                    print "Waiting on $ddn\n";
                    Coro::AnyEvent::sleep(10);
                }

                print "DONE $ddn.\n";
            };
        }
    };
} grep { /^$dbprefix/o } @{$couch->all_dbs->recv};

$_->join foreach @coros;

sub get_all_ddocs { # can probably subclass this
    my $db = shift;
    return $db->all_docs({
        startkey     => '_design/',
        endkey       => '_design0',
        include_docs => 1,
    });
}
