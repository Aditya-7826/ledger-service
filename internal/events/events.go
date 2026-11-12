// Package events defines the events the ledger publishes and the Kafka
// topics they go to.
package events

import (
	"time"

	"github.com/google/uuid"
)

// Event types, topics and the current schema version.
const (
	TypeEntryPosted    = "EntryPosted"
	TypeTransferPosted = "TransferPosted"

	TopicEntries   = "ledger.entries"
	TopicTransfers = "ledger.transfers"

	SchemaVersion = 1

	// TopicPartitions is the partition count of every ledger topic.
	TopicPartitions = 3
)

// Header is carried by every event.
type Header struct {
	EventID       uuid.UUID `json:"event_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
}

// EntryPosted is published once per ledger entry. Its aggregate (and Kafka
// key) is the account, so one account's events stay in order in one partition.
type EntryPosted struct {
	Header
	EntryID     uuid.UUID `json:"entry_id"`
	TransferID  uuid.UUID `json:"transfer_id"`
	AccountID   uuid.UUID `json:"account_id"`
	Direction   string    `json:"direction"`
	AmountMinor int64     `json:"amount_minor"`
	AccountSeq  int64     `json:"account_seq"`
	Currency    string    `json:"currency"`
}

// TransferPosted is published once per transfer. Its aggregate is the transfer.
type TransferPosted struct {
	Header
	TransferID         uuid.UUID  `json:"transfer_id"`
	Type               string     `json:"type"`
	FromAccountID      uuid.UUID  `json:"from_account_id"`
	ToAccountID        uuid.UUID  `json:"to_account_id"`
	AmountMinor        int64      `json:"amount_minor"`
	Currency           string     `json:"currency"`
	Memo               string     `json:"memo"`
	ReversesTransferID *uuid.UUID `json:"reverses_transfer_id"`
}

// NewHeader builds the header of a new event.
func NewHeader(eventType string, occurredAt time.Time) Header {
	return Header{EventID: uuid.Must(uuid.NewV7()), EventType: eventType, SchemaVersion: SchemaVersion, OccurredAt: occurredAt}
}

// Topic returns the topic an event type is published to.
func Topic(eventType string) (string, bool) {
	switch eventType {
	case TypeEntryPosted:
		return TopicEntries, true
	case TypeTransferPosted:
		return TopicTransfers, true
	}
	return "", false
}
