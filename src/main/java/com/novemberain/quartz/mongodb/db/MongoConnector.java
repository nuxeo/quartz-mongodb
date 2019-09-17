package com.novemberain.quartz.mongodb.db;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.quartz.SchedulerConfigException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

/**
 * The responsibility of this class is create a MongoClient with given parameters.
 */
public class MongoConnector {

    private MongoClient mongo;

    private MongoConnector() {
        // use the builder
    }

    public void shutdown() {
        mongo.close();
    }

    public MongoDatabase selectDatabase(String dbName) {
        return mongo.getDatabase(dbName);
    }

    public static MongoConnectorBuilder builder() {
        return new MongoConnectorBuilder();
    }

    public static class MongoConnectorBuilder {
        private MongoConnector connector = new MongoConnector();
        private MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

        private String mongoUri;
        private String username;
        private String password;
        private String[] addresses;
        private String dbName;
        private String authDbName;
        private int writeTimeout;
        private Boolean enableSSL;
        private Boolean sslInvalidHostNameAllowed;
        private String trustStorePath;
        private String trustStorePassword;
        private String trustStoreType;
        private String keyStorePath;
        private String keyStorePassword;
        private String keyStoreType;

        public MongoConnector build() throws SchedulerConfigException {
            connect();
            return connector;
        }

        public MongoConnectorBuilder withClient(MongoClient mongo) {
            connector.mongo = mongo;
            return this;
        }

        public MongoConnectorBuilder withUri(String mongoUri) {
            this.mongoUri = mongoUri;
            return this;
        }

        public MongoConnectorBuilder withCredentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public MongoConnectorBuilder withAddresses(String[] addresses) {
            this.addresses = addresses;
            return this;
        }

        private void connect() throws SchedulerConfigException {
            if (connector.mongo == null) {
                initializeMongo();
            } else {
                if (mongoUri != null  || username != null || password != null || addresses != null){
                    throw new SchedulerConfigException("Configure either a Mongo instance or MongoDB connection parameters.");
                }
            }
        }

        private void initializeMongo() throws SchedulerConfigException {
            connector.mongo = connectToMongoDB();
            if (connector.mongo == null) {
                throw new SchedulerConfigException("Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
            }
            setWriteConcern();
        }

        private MongoClient connectToMongoDB() throws SchedulerConfigException {
            if (mongoUri == null && (addresses == null || addresses.length == 0)) {
                throw new SchedulerConfigException("At least one MongoDB address or a MongoDB URI must be specified .");
            }

            MongoClientOptions.Builder optionBuilder = createOptionBuilder();
            if (mongoUri != null) {
                return connectToMongoDB(mongoUri, optionBuilder);
            }

            return createClient(optionBuilder.build());
        }

        private MongoClient createClient(MongoClientOptions options) throws SchedulerConfigException {
            List<MongoCredential> credentials = createCredentials();
            List<ServerAddress> serverAddresses = collectServerAddresses();
            try {
                return new MongoClient(serverAddresses, credentials, options);
            } catch (MongoException e) {
                throw new SchedulerConfigException("Could not connect to MongoDB", e);
            }
        }

        private MongoClientOptions.Builder createOptionBuilder() throws SchedulerConfigException {
            SSLContext sslContext = getSSLContext();
            if (sslContext == null) {
                if (enableSSL != null) {
                    optionsBuilder.sslEnabled(enableSSL);
                    if (sslInvalidHostNameAllowed != null) {
                        optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
                    }
                }
            } else {
                optionsBuilder.sslEnabled(true);
                if (sslInvalidHostNameAllowed != null) {
                    optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
                }
                optionsBuilder.sslContext(sslContext);
            }
            return optionsBuilder;
        }

        private SSLContext getSSLContext() throws SchedulerConfigException {
            try {
                KeyStore trustStore = loadKeyStore(trustStorePath, trustStorePassword, trustStoreType);
                KeyStore keyStore = loadKeyStore(keyStorePath, keyStorePassword, keyStoreType);
                if (trustStore == null && keyStore == null) {
                    return null;
                }
                SSLContextBuilder sslContextBuilder = SSLContexts.custom();
                if (trustStore != null) {
                    sslContextBuilder.loadTrustMaterial(trustStore, null);
                }
                if (keyStore != null) {
                    sslContextBuilder.loadKeyMaterial(keyStore,
                            StringUtils.isBlank(keyStorePassword) ? null : keyStorePassword.toCharArray());
                }
                return sslContextBuilder.build();
            } catch (GeneralSecurityException | IOException e) {
                throw new SchedulerConfigException("Cannot setup SSL context", e);
            }
        }

        private KeyStore loadKeyStore(String path, String password, String type)
                throws GeneralSecurityException, IOException {
            if (StringUtils.isBlank(path)) {
                return null;
            }
            KeyStore keyStore = KeyStore.getInstance(StringUtils.defaultIfBlank(type, KeyStore.getDefaultType()));
            char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
            try (InputStream is = Files.newInputStream(Paths.get(path))) {
                keyStore.load(is, passwordChars);
            }
            return keyStore;
        }

        private List<MongoCredential> createCredentials() {
            List<MongoCredential> credentials = new ArrayList<MongoCredential>(1);
            if (username != null) {
                if (authDbName != null) {
                    // authenticating to db which gives access to all other dbs (role - readWriteAnyDatabase)
                    // by default in mongo it should be "admin"
                    credentials.add(MongoCredential.createCredential(username, authDbName, password.toCharArray()));
                } else {
                    credentials.add(MongoCredential.createCredential(username, dbName, password.toCharArray()));
                }
            }
            return credentials;
        }

        private List<ServerAddress> collectServerAddresses() {
            List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
            for (String a : addresses) {
                serverAddresses.add(new ServerAddress(a));
            }
            return serverAddresses;
        }

        private MongoClient connectToMongoDB(final String mongoUriAsString, final MongoClientOptions.Builder optionBuilder)
                throws SchedulerConfigException {
            try {
                return new MongoClient(new MongoClientURI(mongoUriAsString, optionBuilder));
            } catch (final MongoException e) {
                throw new SchedulerConfigException("MongoDB driver thrown an exception", e);
            }
        }

        private void setWriteConcern() {
            // Use MAJORITY to make sure that writes (locks, updates, check-ins)
            // are propagated to secondaries in a Replica Set. It allows us to
            // have consistent state in case of failure of the primary.
            //
            // Since MongoDB 3.2, when MAJORITY is used and protocol version == 1
            // for replica set, then Journaling in enabled by default for primary
            // and secondaries.
            WriteConcern writeConcern = WriteConcern.MAJORITY
                    .withWTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                    .withJournal(true);
            connector.mongo.setWriteConcern(writeConcern);
        }

        public MongoConnectorBuilder withAuthDatabaseName(String authDbName) {
            this.authDbName = authDbName;
            return this;
        }

        public MongoConnectorBuilder withDatabaseName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public MongoConnectorBuilder withMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
            if (maxConnectionsPerHost != null) {
                optionsBuilder.connectionsPerHost(maxConnectionsPerHost);
            }
            return this;
        }

        public MongoConnectorBuilder withConnectTimeoutMillis(Integer connectTimeoutMillis) {
            if (connectTimeoutMillis != null) {
                optionsBuilder.connectTimeout(connectTimeoutMillis);
            }
            return this;
        }

        public MongoConnectorBuilder withSocketTimeoutMillis(Integer socketTimeoutMillis) {
            if (socketTimeoutMillis != null) {
                optionsBuilder.socketTimeout(socketTimeoutMillis);
            }
            return this;
        }

        public MongoConnectorBuilder withSocketKeepAlive(Boolean socketKeepAlive) {
            if (socketKeepAlive != null) {
                optionsBuilder.socketKeepAlive(socketKeepAlive);
            }
            return this;
        }

        public MongoConnectorBuilder withThreadsAllowedToBlockForConnectionMultiplier(
                Integer threadsAllowedToBlockForConnectionMultiplier) {
            if (threadsAllowedToBlockForConnectionMultiplier != null) {
                optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(
                        threadsAllowedToBlockForConnectionMultiplier);
            }
            return this;
        }

        public MongoConnectorBuilder withSSL(final Boolean enableSSL, final Boolean sslInvalidHostNameAllowed) {
            this.enableSSL = enableSSL;
            this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
            return this;
        }

        public MongoConnectorBuilder withTrustStore(String trustStorePath, String trustStorePassword, String trustStoreType) {
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
            this.trustStoreType = trustStoreType;
            return this;
        }

        public MongoConnectorBuilder withKeyStore(String keyStorePath, String keyStorePassword, String keyStoreType) {
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.keyStoreType = keyStoreType;
            return this;
        }

        public MongoConnectorBuilder withWriteTimeout(int writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }
    }
}
