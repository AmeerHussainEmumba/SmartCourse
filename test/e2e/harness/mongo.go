package harness

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDatabase is the database the worker writes to (MONGO_DATABASE default).
const MongoDatabase = "smartcourse"

// Mongo reads the archive collections the way an auditor would: straight from
// MongoDB, not through the service that wrote them.
type Mongo struct {
	client *mongo.Client
	db     *mongo.Database
}

func connectMongo(t testing.TB, uri string) *Mongo {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		t.Fatalf("connect to mongodb: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return &Mongo{client: client, db: client.Database(MongoDatabase)}
}

// Count counts the documents of coll matching filter.
func (m *Mongo) Count(coll string, filter bson.M) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.db.Collection(coll).CountDocuments(ctx, filter)
}

// Find returns the documents of coll matching filter.
func (m *Mongo) Find(coll string, filter bson.M) ([]bson.M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := m.db.Collection(coll).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	var out []bson.M
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ArchivedEvents counts archive documents (collection events, keyed by event
// id) for eventID: 1 once archived, never 2.
func (m *Mongo) ArchivedEvents(eventID string) (int64, error) {
	return m.Count("events", bson.M{"_id": eventID})
}

// FailedEvents lists the failed_events documents for eventID, one per consumer
// that gave up on it.
func (m *Mongo) FailedEvents(eventID string) ([]bson.M, error) {
	return m.Find("failed_events", bson.M{"event_id": eventID})
}
