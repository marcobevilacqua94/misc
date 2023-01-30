#pyjnius import and add to classpath Couchbase lite jar libraries 
import jnius_config
jnius_config.add_classpath('/root/couchbase-lite-java-ee-3.0.5-1/lib/*')

from jnius import autoclass

#load java classes we need for this script
CouchbaseLite = autoclass('com.couchbase.lite.CouchbaseLite')
DatabaseConfiguration = autoclass('com.couchbase.lite.DatabaseConfiguration')
Database = autoclass('com.couchbase.lite.Database')
MutableDocument = autoclass('com.couchbase.lite.MutableDocument')
ReplicatorConfiguration = autoclass('com.couchbase.lite.ReplicatorConfiguration')
URLEndpoint = autoclass('com.couchbase.lite.URLEndpoint')
URI = autoclass('java.net.URI')
BasicAuthenticator = autoclass('com.couchbase.lite.BasicAuthenticator')
Replicator = autoclass('com.couchbase.lite.Replicator')
Query = autoclass('com.couchbase.lite.Query')

#initialize Couchbase lite engine
CouchbaseLite.init()

#create/open a db
dbConf = DatabaseConfiguration()
dbConf.setDirectory('/home/marco');
db = Database('databaseName', dbConf)

#create a document with some attributes
document = MutableDocument('documentId')
document.setString('type', 'documentType').setString('owner', 'ownerName')

#save document to db
db.save(document)

#retrieve document
documentRetrieved = db.getDocument('documentId')
documentRetrieved.toJSON()

#set up endpoint for replication (this is the url of my Capella app endpoint)
urlEndpoint = URLEndpoint(URI('wss://b464j7svmtszxuk.apps.cloud.couchbase.com:4984/testpython'))

#these are my use and password for the endpoint
basicAuthenticator = BasicAuthenticator("test", "Pwd12345!")

#create replicator configuration with db, remote endpoint and authentication
replicatorConfiguration = ReplicatorConfiguration(db, urlEndpoint)
replicatorConfiguration.setAuthenticator(basicAuthenticator)

#set replication as continuous (every new document / update will be reflected on remote endpoint, and viceversa, since default replication type is bidirectional)
replicatorConfiguration.setContinuous(True)

#create actual replicator with our configuration
replicator = Replicator(replicatorConfiguration)

#start replication. This replicates our document "documentId" on remote endpoint
replicator.start()

#create new document
document1 = MutableDocument('documentId1').setString('type', 'documentType1').setString('owner', 'ownerName')

#save new document. The replicator replicates this document to the endpoint as well
db.save(document1)

#create a SQL query
query = db.createQuery(
    """
        select *
        from _
        where type = 'documentType1'
    """
)

#executes it
results = query.execute()

#print results
iterator = results.iterator()
while iterator.hasNext():
    print(iterator.next().toJSON())

#prevents script from finishing until 'enter' key is pressed
input()
