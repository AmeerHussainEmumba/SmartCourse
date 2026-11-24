package kafka

// Header is one Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Message is what Producer.Produce sends.
type Message struct {
	// Topic is required.
	Topic string
	// Key selects the partition: the same key always maps to the same
	// partition, so records with one key stay ordered (RFC §6.5). A nil key
	// spreads records across partitions with no ordering.
	Key     []byte
	Value   []byte
	Headers []Header
}

// Result reports where an acknowledged message landed.
type Result struct {
	Partition int32
	Offset    int64
}

// Record is a consumed message, passed to a Handler.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	// Timestamp is the record's timestamp in Unix milliseconds.
	Timestamp int64
}

// Header returns the value of the first header named key.
func (r Record) Header(key string) ([]byte, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}
