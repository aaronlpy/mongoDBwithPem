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
        def username = extractUsernameFromPEM(pemContent)
        MongoCredential credential = MongoCredential.createMongoX509Credential(username, null)
        mongoClient = new MongoClient(serverAddress, [credential] as List)
        database = mongoClient.getDatabase(databaseName)
    }

    private String extractUsernameFromPEM(String pemContent) {
        // Extract the common name (CN) from the PEM content
        // This assumes the PEM content follows the X.509 certificate format
        // You may need to adjust this logic based on your certificate format
        def matcher = (pemContent =~ /CN=([^,]+)/)
        if (matcher.find()) {
            return matcher.group(1)
        }
        throw new IllegalArgumentException("Cannot extract username from PEM file.")
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
