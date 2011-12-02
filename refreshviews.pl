use common::sense;
use Coro;
use Coro::AnyEvent;
use AnyEvent;
use AnyEvent::CouchDB;

my $dbprefix = 'ws228_experimental';

my $couch = AnyEvent::CouchDB->new; # localhost 5984

my @coros = map {
    my $db = $couch->db($_);
    my $dbname = $db->name;
    async {
        my $ddoc = ${ get_all_views($db)->recv->{rows} }[0]->{doc}
            or return;
        print "Refreshing $dbname\n";
        my ($arbitrary_view) = keys %{$ddoc->{views}};
        (my $model = $ddoc->{_id}) =~ s{_design/}{};

        # try getting the view until it's generated
        my $result;
        while (!($result = eval { $db->view($model . '/' . $arbitrary_view)->recv })) {
            print "Waiting on $dbname\n";
            Coro::AnyEvent::sleep(10);
        };

        print "DONE $dbname.\n";
    };
} grep { /^$dbprefix/o } @{$couch->all_dbs->recv};

$_->join foreach @coros;

sub get_all_views { # can probably subclass this
    my $db = shift;
    return $db->all_docs({
        startkey     => '_design/',
        endkey       => '_design0',
        include_docs => 1,
    });
}
