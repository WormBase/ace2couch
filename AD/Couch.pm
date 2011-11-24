package AD::Couch;

# simple Couch interface for loading documents into a CouchDB
# supporting bulkloading

use strict;
use warnings;
use LWP::UserAgent;
use HTTP::Request::Common;
use HTTP::Headers;
use JSON;

use constant DEFAULT_HOST      => 'localhost';
use constant DEFAULT_PORT      => 5984;
use constant DEFAULT_DATABASE  => 'test';
use constant DEFAULT_BLOCKSIZE => 1_000_000;

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
    $self->blocksize($args{blocksize} || $args{block_size} ||
                     $args{bs} || DEFAULT_BLOCKSIZE);
    $self->refresh_views_on_flush($args{refresh_views_on_flush});
    $self->{nocheck} = $args{nocheck};

    if ($args{agent}) {
        $self->agent($args{agent});
    }
    else {
        $self->agent(LWP::UserAgent->new(
            keep_alive      => 10,
            timeout         => 300,
        ));
    }

    return unless $self->_make_database;

    $self->{_blocks} = [];
    $self->{_blocks_size} = 0;

    return $self;
}

sub DESTROY {
    my $self = shift;
    if (@{$self->{_blocks}}) {
        $self->_flush_blocks;
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
    if ($self->_add_block($doc, $size) >= $self->blocksize) {
        return 0 unless $self->_flush_blocks;
    }
    return 1;
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

sub blocksize {
    my ($self, $bs) = @_;
    return $self->{blocksize} = $bs // $self->{blocksize};

}

sub agent {
    my ($self, $ag) = @_;
    return $self->{agent} = $ag || $self->{agent};
}

sub dburl {
    my $self = shift;
    return "http://" . $self->host . ':' . $self->port . '/' . $self->db;
}

sub refresh_views_on_flush {
    my ($self, $refresh) = @_;
    return $self->{refresh_views_on_flush}
        = $refresh // $self->{refresh_views_on_flush};
}

sub get_all_design_docs {
    my $self = shift;

    my ($json, $response);
    my $url = $self->dburl . '/_all_docs?startkey="_design%2F"&endkey="_design%2Fzzzzzzzzzzz"'
            . '&include_docs=true';
    $response = $self->agent->request(GET $url);

    # error-handling

    my $data = decode_json($response->content);
    return { map { $_->{_id} => $_ } map { $_->{doc} } @{$data->{rows}} };
}

sub refresh_all_views {
    my $self = shift;

    my $ddocs = $self->{_design_docs} //= $self->get_all_design_docs;

    for my $ddoc_id (sort keys %$ddocs) {
        my ($arbitrary_view) = keys %{$ddocs->{$ddoc_id}->{views}};
        my $url = join('/', $self->dburl, $ddoc_id, '_view', $arbitrary_view);
        my $res = $self->agent->request(GET $url);
        if ($res->is_success) {
            warn "View $ddoc_id got refreshed\n";
        }
        else {
            warn "View $ddoc_id FAILED to refresh\n", $res->status_line, "\n",
                $res->content, "\n", $url, "\n";
        }
    }
}

sub _add_block {
    my ($self, $block, $size) = @_;
    $size //= $self->_size_of($_);

    push @{$self->{_blocks}}, $block;
    $self->{_blocks_size} += $size;

    return $self->{_blocks_size};
}

sub _make_database {
    my $self = shift;

    my ($json, $response);

    $response = $self->agent->request(GET $self->dburl);
    $json = decode_json($response->content);

    if ($json->{error} and $json->{reason} eq 'no_db_file') {
        # try making the db
        $response = $self->agent->request(PUT $self->dburl);
        $json = decode_json($response->content);
        if ($json->{error}) {
            $self->error("Could not make database ", $self->db,
                         ' : ', $json->{reason});
            return;
        }
        return 1;
    }

    if ($json->{error}) {
        $self->error("Could not make database ", $self->db, ':',
                     $json->{reason} || "Can't get connection to " . $self->dburl);
        return;
    }

    return 1;
}

sub _flush_blocks {
    my ($self, $try_once) = @_;

    my ($json, $response);
    my $blocks = $self->{_blocks};

    unless ($self->{nocheck}) {
        # fetch the revisions so we can update if necessary
        $json = encode_json({ keys => [ map { $_->{_id} // () } @$blocks ] });
        $response = $self->agent->request(
            POST $self->dburl . '/_all_docs',
            Content_Type => 'application/json',
            Content      => $json,
        );

        my $rows = eval { decode_json($response->content)->{rows} };
        if (!$rows) {
            $self->error("Problem with decoding JSON.\n",
                         "Status line: ", $response->status_line, "\n",
                         "Content: ", $response->content);
            return if $try_once;
            return $self->_flush_blocks(1);
        }

        my %revs = map { $_->{id} => $_->{value}{rev} }
                   grep { ! $_->{error} } @$rows;

        # if a doc exists on db and needs to be updated, add rev info
        foreach my $doc (@$blocks) {
            $doc->{_rev} = $revs{$doc->{_id}} if exists $revs{$doc->{_id}};
        }
    }

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

    @{$self->{_blocks}} = ();
    $self->{_blocks_size} = 0;

    if ($self->refresh_views_on_flush) {
        $self->refresh_all_views;
    }

    return $count;
}

sub _size_of {
    my (undef, $obj) = @_;
    return length $obj->{Body} if $obj->{Body};
    return length encode_json($obj);
}

1;
