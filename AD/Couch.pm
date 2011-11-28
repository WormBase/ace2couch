package AD::Couch;

# simple Couch interface for loading documents into a CouchDB
# supporting bulkloading

use strict;
use warnings;
use AnyEvent::CouchDB;
use Coro;
use JSON;

use constant DEFAULT_HOST            => 'localhost';
use constant DEFAULT_PORT            => 5984;
use constant DEFAULT_DATABASE        => 'test';
use constant DEFAULT_MAX_BUFFER_SIZE => 1_000_000;

BEGIN {
    *new = \&connect;
    *db  = \&database;
}

sub connect {
    my $class = shift;
    my %args = (@_ == 1 ? %{$_[0]} : @_);
    my $self = bless {}, $class;

    $self->host($args{host} || DEFAULT_HOST);
    $self->port($args{port} || DEFAULT_PORT);
    $self->database($args{database} // $args{db} // DEFAULT_DATABASE);
    $self->max_buffer_size($args{max_buffer_size} || DEFAULT_MAX_BUFFER_SIZE);
    $self->refresh_views_on_flush($args{refresh_views_on_flush});

    $self->{_agent} = AnyEvent::CouchDB->new('http://'.$self->host.':'.$self->port.'/');
    $self->{_db} = $self->{_agent}->db($self->database);

    # check for the DB and make if necessary
  FINDDB: {
        my $dbs = $self->{_agent}->all_dbs->recv;
        foreach (@$dbs) {
            last FINDDB if $self->database eq $_;
        }
        $self->{_db}->create->recv;
    }

    $self->{_buffer} = [];
    $self->{_buffer_size} = 0;

    return $self;
}

sub DESTROY {
    my $self = shift;
    if (@{$self->{_buffer}}) {
        $self->_flush_buffer;
    }
}

sub error {
    my $self = shift;
    warn @_;
    return;

    push @{$self->{error}}, join('', @_) if @_;
    return unless @{$self->{error}};

    my $tmp = $self->{error};
    $self->{error} = [];

    return wantarray ?  @$tmp : $tmp->[-1];
}

sub add_doc {
    my ($self, $doc, $size) = @_;
    $doc = { Body => $doc } unless ref $doc eq 'HASH';

    # _add_block will compute (guess) the size if not provided
    if ($self->_add_block($doc, $size) >= $self->max_buffer_size) {
        $self->_flush_buffer;
    }
}

sub port {
    my ($self, $port) = @_;
    return $self->{port} = $port // $self->{port};
}

sub host {
    my ($self, $host) = @_;
    return $self->{host} = $host // $self->{host};
}

sub database {
    my ($self, $db) = @_;
    return $self->{database} = $db // $self->{database};
}

sub max_buffer_size {
    my ($self, $bs) = @_;
    return $self->{max_buffer_size} = $bs // $self->{max_buffer_size};

}

sub refresh_views_on_flush {
    my ($self, $refresh) = @_;
    return $self->{refresh_views_on_flush}
        = $refresh // $self->{refresh_views_on_flush};
}

sub get_all_design_docs {
    my $self = shift;

    my $data = $self->{_db}->all_docs({
        startkey     => '_design/',
        endkey       => '_design/zzzzzzzzzzzzzzzzzzzzzzzzzz',
        include_docs => 1,
    })->recv; # this can fail... should handle

    return { map { $_->{_id} => $_ } map { $_->{doc} } @{$data->{rows}} };
}

sub refresh_all_views {
    my $self = shift;

    my $ddocs = $self->{_design_docs} //= $self->get_all_design_docs;

    my @coros = map {
        my $ddoc_id= $_;
        async {
            my ($arbitrary_view) = keys %{$ddocs->{$ddoc_id}->{views}};
            (my $model = $ddoc_id) =~ s{_design/}{};

            # try getting the view until it's generated
            my $count = 0;
            my $result;
            while (! eval { $self->{_db}->view($model . '/' . $arbitrary_view)->recv }) {
                last if ++$count > 10; # hardcoded...
                Coro::AnyEvent::sleep(10);
            }
        };
    } sort keys %$ddocs;

    $_->join foreach @coros;
}

sub _add_block {
    my ($self, $block, $size) = @_;
    $size //= $self->_size_of($_);

    push @{$self->{_buffer}}, $block;
    return $self->{_buffer_size} += $size;
}

sub _flush_buffer {
    my $self = shift;

    $self->{_db}->bulk_docs($self->{_buffer})->recv;
    if ($self->refresh_views_on_flush) {
        $self->refresh_all_views;
    }
    my $num_docs = @{$self->{_buffer}};
    @{$self->{_buffer}} = ();
    $self->{_buffer_size} = 0;

    return $num_docs;
}

sub _size_of {
    my (undef, $obj) = @_;
    return length $obj->{Body} if $obj->{Body};
    return length encode_json($obj);
}

## OLD DEPRECATED STUFF

sub _old_flush_buffer {
    my ($self, $try_once) = @_;

    my ($json, $response);
    my $blocks = $self->{_buffer};

    my @ranges = ( [0, $#$blocks] ); # try all the blocks

    my $count = 0;

    while (@ranges) {
        my @cur_range = @{shift @ranges};
        my @blocks = @{$blocks}[$cur_range[0] .. $cur_range[1]]; # take a slice of the blocks

        $json = encode_json({ docs => \@blocks, all_or_nothing => JSON::true });
        $response = $self->agent->request(
            POST $self->dburl . '/_bulk_docs',
            Content_Type => 'application/json',
            Content      => $json,
        );

        if ($response->is_error) {
            $self->error("Unsuccessful flush: ", $response->status_line);
            if ($response->status_line =~ /timeout/io and @blocks > 1) {
                push @ranges, [$cur_range[0], $cur_range[1]/2],
                              [$cur_range[1]/2 + 1, $cur_range[1]];
            }
        }
        else {
            $count += @blocks;
        }
    }

    @{$self->{_buffer}} = ();
    $self->{_buffer_size} = 0;

    if ($self->refresh_views_on_flush) {
        $self->refresh_all_views;
    }

    return $count;
}

1;
