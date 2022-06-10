package lib

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Simplified interface for srclient.SchemaRegistryClient
type SchemaRegistryClient interface {
	GetSchema(schemaID int) (*srclient.Schema, error)
	GetLatestSchema(subject string) (*srclient.Schema, error)
	SetCredentials(username string, password string)
}

// SchemaResolver is an interface that can resolve a schema registry schema from
// destination topic and the entity being serialized. It is analogous to the
// SubjectNameStrategy in confluent clients, but also performs the registry schema
// lookup.
type SchemaResolver interface {
	ResolveSchema(topic string, msg proto.Message) (*srclient.Schema, error)
}

// SerializationType is a type alias for representing Key and Value serialization
// types
type SerializationType int

const (
	KeySerialization SerializationType = iota
	ValueSerialization
)

// TopicNameSchemaResolver is an instance of SchemaResolver which uses the topic
// name as the subject when looking up schema via schema registry
type TopicNameSchemaResolver struct {
	serializationType SerializationType
	client            SchemaRegistryClient
}

// NewTopicNameSchemaResolver is a constructor for TopicNameSchemaResolver.
// Receives a SchemaRegistryClient, which should have caching enabled as schema
// is resolved for every serialization performed by a serializer, as well as a
// SerializationType, which specifies whether to resolve a key or value schema
// for the topic
func NewTopicNameSchemaResolver(
	client SchemaRegistryClient,
	serializationType SerializationType,
) *TopicNameSchemaResolver {
	return &TopicNameSchemaResolver{
		serializationType: serializationType,
		client:            client,
	}
}

// ResolveSchema using the TopicNameStrategy, which uses the topic name as the
// subject. Ensure the schema registry client that was pass to the constructor
// has caching enabled or this will be slow to execute
func (ls *TopicNameSchemaResolver) ResolveSchema(
	topic string,
	msg proto.Message,
) (*srclient.Schema, error) {
	if msg == nil {
		return nil, fmt.Errorf(
			"cannot resolve schema for nil message and topic: %s",
			topic,
		)
	}
	return ls.client.GetLatestSchema(ls.constructSubject(topic))
}

func (ls *TopicNameSchemaResolver) constructSubject(topic string) string {
	if ls.serializationType == KeySerialization {
		return topic + "-key"
	}
	return topic + "-value"
}

// SerializationFunc is a type that describes the function that is ultimately
// used to serialize a protobuf.
type SerializationFunc = func([]byte, proto.Message) ([]byte, error)

// InitializationFunc is a type that describes a function to be used to initialize
// a messsage prior to serialization.
type InitializationFunc = func(proto.Message)

// ProtobufSerializer is an instance of Serializer which serializes protobufs
// according to the confluent schema registry line protocol
type ProtobufSerializer struct {
	schemaResolver SchemaResolver
	headerCache    map[int]map[string][]byte
	marshal        SerializationFunc
	initialize     InitializationFunc
}

// VTMarshal is an interface that will be satisfied by any protobuf that has had
// the protoc-gen-go-vtproto plugin applied to it with the marshal and size
// options. If a proto satisfies this interface, the Marshal function will apply
// the much more efficient MarshalToVT serialization
type VTMarshal interface {
	SizeVT() int
	MarshalToVT(data []byte) (int, error)
}

// Marshal is a wrapper around proto which will use MarshalToVT if that
// method is available in the proto, which serializes much more rapidly
// than the reflection-based proto.Marshal
func Marshal(header []byte, msg proto.Message) ([]byte, error) {
	switch m := msg.(type) {
	case VTMarshal:
		// Whenever available, use VTMarshal for MUCH faster serialization
		size := len(header) + m.SizeVT()
		buffer := make([]byte, 0, size)
		buffer = append(buffer, header...)
		bytesWritten, err := m.MarshalToVT(buffer[len(header):])
		return buffer[:len(header)+bytesWritten], err
	default:
		bytes, err := proto.Marshal(msg)
		header = append(header, bytes...)
		return header, err
	}
}

// NewProtobufSerializer is a constructor function for ProtobufSerializer.
// Receives a SchemaResolver as parameter.
func NewProtobufSerializer(
	schemaLookupStrategy SchemaResolver,
	initialize InitializationFunc,
	serializationFunc ...SerializationFunc,
) *ProtobufSerializer {
	// marshall via Marshal by default
	marshal := Marshal
	if len(serializationFunc) > 0 {
		marshal = serializationFunc[0]
	}

	return &ProtobufSerializer{
		schemaResolver: schemaLookupStrategy,
		headerCache:    make(map[int]map[string][]byte),
		marshal:        marshal,
		initialize:     initialize,
	}
}

// Serialize encodes a protobuf for the specified topic.
func (ps *ProtobufSerializer) Serialize(
	topic string,
	thing interface{},
) ([]byte, error) {
	if thing == nil {
		// It is legitimate to serialize nil to nil
		return nil, nil
	}

	// ensure thing is a protobuf
	var msg proto.Message = nil
	switch t := thing.(type) {
	case proto.Message:
		msg = t
	default:
		return nil, fmt.Errorf(
			"serialization target must be a protobuf. Got '%v'",
			t,
		)
	}

	schema, err := ps.schemaResolver.ResolveSchema(topic, msg)
	if err != nil {
		return nil, err
	}

        // initialize(msg) is a user-provided function which can initialize fields in the empty protobuf.
        // A timestamp or a source address or anything else your particular use case may require.
        // We have a Metadata message type in all our protos which is always in a top-level field
        // called 'metadata' which we populate reflectively via initialize().  It contains information 
        // about the source process. We could put that info in headers instead of a field of the
        // message. 
	if ps.initialize != nil {
		ps.initialize(msg)
	}

	// avoid recomputing message indexes and encoding header on every request
	buf := ps.lookupHeader(schema.ID(), msg)
	if buf == nil {
		msgIndexes := computeMessageIndexes(msg.ProtoReflect().Descriptor(), 0)
		buf = encodePayloadHeader(schema.ID(), msgIndexes)

		ps.storeHeader(schema.ID(), msg, buf)
	}

	bytes, err := ps.marshal(buf, msg)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (ps *ProtobufSerializer) lookupHeader(id int, msg proto.Message) []byte {
	messageTypes := ps.headerCache[id]
	if messageTypes == nil {
		return nil
	}
	return messageTypes[string(msg.ProtoReflect().Descriptor().FullName())]
}

func (ps *ProtobufSerializer) storeHeader(
	id int,
	msg proto.Message,
	header []byte,
) {
	// populate the cache of pre-encoded headers
	messageTypes := ps.headerCache[id]
	if messageTypes == nil {
		messageTypes = make(map[string][]byte)
		ps.headerCache[id] = messageTypes
	}
	messageTypes[string(msg.ProtoReflect().Descriptor().FullName())] = header
}

// protobuf line protocol for kafka has protocol version number (0 as byte),
// then schema id (uint32), then an array of message indexes that eventually
// identifies exactly which message within a schema file the proto in question
// actually is. If proto is 3rd message nested within message that is 4th
// message within first message in schema file, array would be [0, 3, 2].
// First message in schema is [0]
func computeMessageIndexes(
	descriptor protoreflect.Descriptor,
	count int,
) []int {
	index := descriptor.Index()
	switch v := descriptor.Parent().(type) {
	case protoreflect.FileDescriptor:
		// parent is FileDescriptor, we reached the top of the stack, so we are
		// done. Allocate an array large enough to hold count+1 entries and
		// populate first value with index
		msgIndexes := make([]int, count+1)
		msgIndexes[0] = index
		return msgIndexes[0:1]
	default:
		// parent is another MessageDescriptor.  We were nested so get that
		// descriptor's indexes and append the index of this one
		msgIndexes := computeMessageIndexes(v, count+1)
		return append(msgIndexes, index)
	}
}

// encodePayloadHeader writes the line protocol header for protobufs, which
// consists of the protocol version (0 as byte), the schema id (uint32),
// followed by the length of the message index array (variable, zigzag
// encoded) and then each element of that array (variable, zigzag encoded).
func encodePayloadHeader(schemaId int, msgIndexes []int) []byte {
	// allocate buffer with 5 bytes for version and schemaId, and sufficient
	// space for msgIndexes in zigzag encoding plus length of array
	buf := make([]byte, 5+((1+len(msgIndexes))*binary.MaxVarintLen64))

	// write version of protobuf line protocol
	buf[0] = byte(0)

	// write schema id
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaId))
	length := 5

	// write length of indexes array
	length += binary.PutVarint(buf[length:], int64(len(msgIndexes)))

	// Now write each array value
	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}

	return buf[0:length]
}

type StringSerializer struct {
}

func (s *StringSerializer) Serialize(topic string, thing interface{}) ([]byte, error) {
	return []byte(thing.(string)), nil
}

type JsonSerializer struct {
}

func (s *JsonSerializer) Serialize(topic string, thing interface{}) ([]byte, error) {
	return json.Marshal(thing)
}