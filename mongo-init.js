db = db.getSiblingDB('analytics');

// Username/password are read from environment variables passed into the
// mongo container (see docker-compose.yml -> mongo -> environment).
// mongosh runs on Node.js, so process.env works here directly.
const appUsername = process.env.MONGO_APP_USERNAME;
const appPassword = process.env.MONGO_APP_PASSWORD;

if (!appUsername || !appPassword) {
  throw new Error(
    'MONGO_APP_USERNAME / MONGO_APP_PASSWORD are not set. ' +
    'Make sure your .env file is filled in before starting the containers.'
  );
}

db.createUser({
  user: appUsername,
  pwd: appPassword,
  roles: [{ role: 'readWrite', db: 'analytics' }]
});

// Collection for raw API responses
db.createCollection('raw_tweets');

// Create indexes for query performance
db.raw_tweets.createIndex({ "timestamp": -1 });
db.raw_tweets.createIndex({ "source": 1 });

print('MongoDB collections created!');
