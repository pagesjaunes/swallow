Swallow
========

Swallow is a python framework that aims to make data transformation easier and faster: it allows to focus on the transformation process, and to get
benefits of existing connector that will retreive/push data from/to a base/collection/index.

# Description
Swallow is composed of a main class (Swallow) and of many "io connectors".
It runs multiple workers that communicate with two queues
* Readers get data from a source and push it into the "in_queue"
* Processors get data from "in_queue", transform it and push it into the "out_queue"
* Writers get data from "out_queue" and store it into a destination base or file
* Readers are objects defining a "scan_and_queue" method according to the following signature : scan_and_queue(self,p_queue,**kwargs). This typically scans a base/file/collection/index and puts each document/item into the p_queue
* Writers are objects defining a "dequeue_and_store" method according to the following signature : dequeue_and_store(self,p_queue,**kwargs).This typically gets documents/items from a queue and stores them into a base/file/collection/index
* Processors are functions that transform a reader format doc into the writer expected format. They must have the signature : function_name(p_srcDoc,*args) and must return a list of doc in the expected format

Note that as they consume a queue, both writers and processors must deal the "poison pill" item. Once they get a "None" item from
the list they are consuming, they must stop to listen to it.
The Swallow object automatically generates these "pills" as it knows when producers have finished their task.

# Example of use
Get data from elastic search to a csv file

```python
    from swallow import Swallow
    # Transforms a doc from the es index to a csv row
    def create_csv_row(p_srcdoc,*args):
        csv_row = []
        csv_row.append(p_srcdoc['field_for_col1'])
        csv_row.append(p_srcdoc['field_for_col2'])
        return [csv_row]

    nb_threads = 5
    es_reader = ESio('127.0.0.1','9200',1000)
    csv_writer = CSVio()

    swal = Swallow()
    swal.set_reader(es_reader,p_index='my_es_index',p_doctype='my_doc_type',p_query={})
    swal.set_writer(csv_writer,p_file=arguments['--csv'])
    swal.set_process(create_csv_row)

    swal.run(nb_threads)
```

Ce module proclame la bonne parole de sieurs Sam et Max. Puissent-t-ils
retrouver une totale liberté de pensée cosmique vers un nouvel age
reminiscent.

# Connector doc
## Connect Mongo

### Create the object

The constructor takes these parameters :

    * p_host : Mongo Server host. If using a replicaset, can be the Master Node.
    * p_port : Mongo Server port. If using a replicaset, can be the Master Node Port.
    * p_base : Mongo base name.
    * p_user : User that access the mongo base
    * p_password : Passwd for accessing the mongo base
    * p_connect_timeout (default 60000) : timeout in MS before closing a connection
    * p_rs_xtra_nodes=None (default None) : replicaset node list (added to the host given with p_host) : comma separated string list ("host:port","host:port", ...)
    * p_rs_name=None (default None) : replicaset name

```python
# Simple connection
mongo_connector = Mongoio('localhost',27017,'myBase','user','passwd')

# Replicaset "foo" connection
mongo_connector = Mongoio('localhost',27017,'myBase','user','passwd',p_rs_name="foo")

# Replicaset "foo" connection with extra hosts
mongo_connector = Mongoio('localhost',27017,'myBase','user','passwd',p_rs_name="foo",p_rs_xtra_nodes=['localhost:27018','localhost:27019'])
```

Then use the object for reading or storing elements

####Reading :

    * p_collection : Collection where items are picked from
    * p_query : MongoDB query for scanning the collection
    * p_batch_size (default 100) : Number of read docs by iteration
 
```python
# Reading all doc from "myCollection"
swal.set_reader(mongo_connector,p_collection='myCollection',p_query={})
```

####Storing :

* p_collection: mongo collection where to store the docs

```python
# Writting to myCollection
swal.set_writer(mongo_connector,p_collection='myIndex')
```

## Connect ElasticSearch
This copy an index from an host to another

```python
# Swallow instance : deals with queues and multiprocessing
swal = Swallow()

# reader ES
# host, port, bulk_size
reader = ESio('localhost',9200,1000)

# writer is ES too
writer = ESio('anotherhost',10200,1000)

# p_query = {} => select all the doc from my_index
swal.set_reader(reader,p_index=my_index,p_query={})
swal.set_writer(writer,p_index=a_new_index)
swal.set_process(pass_doc)

swal.run(4)
```

with this process function :

```python
def pass_doc(p_srcdoc):

    document = {
        "_type": p_srcdoc['_type'],
        "_source": p_srcdoc['_source'],
        "_id": p_srcdoc['_id']
    }

    return [document]
```

# Install

The easiest way is to run the pip command :

```
pip install swallow
```

# Tests

There are few unit tests but they keep growing !
They required py.test to be ran properly. From the home directory just launch :

```
py.test test/
```

# Python version
This lib requires python 3.2+

# License

This project is released under version 2.0 of the [Apache License][]

# About the project name

It refers to Holy Grail and King Arthur talking about African Swallows. This framework transmits and transforms data from queue to queue, as the original swallow carried coconuts.

# MacOS Users

Be careful : on MacOS, multiprocessing.Queue max size is limited to 32767 items. Make sure you init the Swallow object with no more than 32767. If you don't, you'll get a OSError 22.