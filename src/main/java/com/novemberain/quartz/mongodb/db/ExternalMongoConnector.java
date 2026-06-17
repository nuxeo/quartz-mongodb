package com.novemberain.quartz.mongodb.db;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * The implementation of {@link MongoConnector} that doesn't own the lifecycle of {@link MongoClient}.
 */
public class ExternalMongoConnector implements MongoConnector {

    private final WriteConcern writeConcern;
    private final MongoDatabase database;

    /**
     * Constructs an instance of {@link ExternalMongoConnector}.
     *
     * @param writeConcern instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                     {@link #getCollection(String)} will be configured with this write concern.
     * @param database     {@link MongoDatabase} instance. Will be used to produce collections.
     */
    public ExternalMongoConnector(final WriteConcern writeConcern, final MongoDatabase database) {
        this.database = database;
        // Pin to ACKNOWLEDGED if null so the driver treats it as the server default
        // (WriteConcern.isServerDefault() == true) and omits the writeConcern field from commands.
        // Required for DocumentDB Elastic, which rejects commands carrying an explicit
        // writeConcern (error 303). Also covers the case where the externally-provided
        // MongoClient/MongoDatabase was constructed without an explicit writeConcern.
        this.writeConcern = writeConcern != null ? writeConcern : WriteConcern.ACKNOWLEDGED;
    }

    /**
     * Constructs an instance of {@link ExternalMongoConnector}.
     *
     * @param writeConcern instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                     {@link #getCollection(String)} will be configured with this write concern.
     * @param mongoClient  instance of {@link MongoClient}.
     * @param dbName       name of the database that will be used to produce collections.
     */
    public ExternalMongoConnector(final WriteConcern writeConcern, final MongoClient mongoClient, final String dbName) {
        this(writeConcern, mongoClient.getDatabase(dbName));
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return database.getCollection(collectionName).withWriteConcern(writeConcern);
    }

    @Override
    public MongoDatabase getDatabase() {
        // Database-level commands (e.g. createCollection) also need the writeConcern field
        // omitted when targeting DocumentDB Elastic; the field's invariant guarantees that.
        return database.withWriteConcern(writeConcern);
    }

    @Override
    public void close() {
        // we don't own the lifecycle of MongoClient, ignore.
    }

}
