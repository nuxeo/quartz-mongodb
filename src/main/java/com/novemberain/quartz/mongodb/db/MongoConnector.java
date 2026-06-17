package com.novemberain.quartz.mongodb.db;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.Closeable;

/**
 * Interface through which quartz-mongodb interacts with MongoDB.
 */
public interface MongoConnector extends Closeable {

    /**
     * Quartz-mongodb will call this method to get the instance of {@link MongoCollection} for internal uses.
     * The collection is expected to be fully configured with correct {@link WriteConcern}.
     *
     * @param collectionName collection name.
     * @return instance of {@link MongoCollection}.
     */
    MongoCollection<Document> getCollection(final String collectionName);

    /**
     * Returns the underlying {@link MongoDatabase}, used for DDL operations such as explicit collection creation.
     * <p>
     * Implementations should ensure the returned database does not carry an explicit (non-server-default)
     * write concern, so the MongoDB driver omits the {@code writeConcern} field from DDL commands.
     * This is required for AWS DocumentDB Elastic, which rejects any command carrying a {@code writeConcern}
     * field (error 303). Built-in connectors pin the returned database to {@link WriteConcern#ACKNOWLEDGED}
     * (the driver server default) to satisfy this contract.
     * <p>
     * Custom implementations that do not override this method will throw {@link UnsupportedOperationException}
     * at runtime. Override this method if your deployment requires explicit collection creation
     * (e.g. Amazon DocumentDB Elastic).
     *
     * @return instance of {@link MongoDatabase}.
     */
    default MongoDatabase getDatabase() {
        throw new UnsupportedOperationException(
                "getDatabase() is not implemented by this MongoConnector. "
                        + "Override this method to support explicit collection creation (e.g. for DocumentDB Elastic).");
    }

    /**
     * Quartz-mongodb will call this method when shutting down.
     * Implementation can close {@link MongoClient} here.
     */
    @Override
    void close();
}
