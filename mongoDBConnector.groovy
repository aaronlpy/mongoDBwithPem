import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

class MongoDBConnector {
    MongoClient mongoClient
    MongoDatabase database

    MongoDBConnector(String host, int port, String databaseName, String pemFilePath) {
        ServerAddress serverAddress = new ServerAddress(host, port)
        def pemFile = new File(pemFilePath)
        def pemContent = pemFile.text
        MongoCredential credential = MongoCredential.createMongoX509Credential(null, pemContent.toCharArray())
        mongoClient = new MongoClient(serverAddress, [credential] as List)
        database = mongoClient.getDatabase(databaseName)
    }

    MongoCollection<Document> getCollection(String collectionName) {
        return database.getCollection(collectionName)
    }
}

// Example usage:
def connector = new MongoDBConnector("localhost", 27017, "myDatabase", "/path/to/pem/file.pem")
def collection = connector.getCollection("myCollection")

// Perform operations on the collection, e.g., insert document
def document = new Document("name", "John Doe").append("age", 30)
collection.insertOne(document)

// Close the MongoClient when done
connector.mongoClient.close()
