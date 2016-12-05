# Israel O. Dilán
# *801-11-2035*


### Metadata Server

The metadata server works as a man in the middle for every operation in the
distrubuted filesystem. It's job is to keep track of every change by constantly
updating the database with information of files being stored or deleted from the
dfs and providing this information back to the client when requested.

### Data Nodes

The data nodes act as distrubuted storage point to which pieces of a given file are
sent and are stored there indexed only by the metadata server. To reach a data node
or a stored fragment of a file, it is neccesarry to first contact the metadata
server. 

### List Client

The list python program allows users to list what files are currently stored on
the distributed filesystem. It works by sending and encoded packet from the ls.py
client to the meta data server, this packect contains the command list which then
utilizes mds_db.py methods 

### Copy Client

The copy python program helps the user to add files to the dfs and to retrieve 
them. To do so the client must first contact the metadataserver which will then
return information of available data nodes and the blocks stored within them as
requested, including the necesary information to contact the data node and request
a copy of a block or to store a new block.  

### Delete Client

The delete python program contacs the metadata server to remove a specified file.
In turn the metadata server returns the addresses of the data nodes containing the
blocks of the to be deleted and updates the database to remove all reference to the
file that has been deleted from it. The client then goes on to contact each node to
ask for the removal of the file.