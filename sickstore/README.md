# SickStore Driver for YCSB
This driver is a binding for the YCSB facilities to operate against [SickStore](https://github.com/steffenfriedrich/SickStore).



### 1. Start SickStore Server
First, download [SickStore](https://github.com/steffenfriedrich/SickStore)  and start the server:

    $ curl -O --location http://nosqlmark.informatik.uni-hamburg.de/sickstore-1.5.tar.gz
    $ tar xfvz sickstore-1.5.tar.gz
    $ cd sickstore-1.5

## Configuration Options

### YCSB-binding
 - sickstore.url=localhost => The connection URL.
 - sickstore.port=54000 => 
 - sickstore.maxconnections=64 => number of connections in the connection pool
 - sickstore.write_concern.ack=1 => The number of acknowledgments from replicas (or a tag set)
 - sickstore.write_concern.journaling=false => Simulate a journal commit?
 - sickstore.read_preference =>  primary | secondary
