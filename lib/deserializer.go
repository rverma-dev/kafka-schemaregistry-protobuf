package lib

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtobufResolver is an interface which can resolve a protobuf
// MessageDescriptor from topic name and the info contained in the
// message header and instantiate an instance of the message described
// by the MessageDescriptor
type ProtobufResolver interface {
	ResolveProtobuf(topic *string, schemaId uint32, msgIndexes []int64) (proto.Message, error)
}

// ProtobufRegistry is the minimum interface of protoregistry.Types registry
// needed to resolve MessageType from topic name (plus a registration function,
// for convenience)
type ProtobufRegistry interface {
	RangeMessages(f func(protoreflect.MessageType) bool)
	FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error)
}

// DeserializationType is a type alias for representing Key and Value
// deserialization types
type DeserializationType int

const (
	KeyDeserialization DeserializationType = iota
	ValueDeserialization
)

// ProtoregistryTopicNameProtobufResolver is a concrete implementation of
// ProtobufResolver which uses topic name in combination with protoregistry
// to resolve a protoreflect.MessageType that can then be used to instantiate
// a new instance of that type
type ProtoregistryTopicNameProtobufResolver struct {
	deserializationType DeserializationType
	registry            ProtobufRegistry
}

// NewProtoregistryTopicNameProtobufResolver is a constructor
func NewProtoregistryTopicNameProtobufResolver(
	registry ProtobufRegistry,
	deserializationType DeserializationType,
) *ProtoregistryTopicNameProtobufResolver {
	return &ProtoregistryTopicNameProtobufResolver{
		deserializationType: deserializationType,
		registry:            registry,
	}
}

// ResolveProtobuf uses topic name in combination with protorregistry to find
// protoreflect.MessageType that matches.  It then instantiates a new instance ot
// that type and returns it.
func (reg *ProtoregistryTopicNameProtobufResolver) ResolveProtobuf(
	topic *string,
	schemaId uint32,
	msgIndexes []int64,
) (proto.Message, error) {
	var mt protoreflect.MessageType
	reg.registry.RangeMessages(func(messageType protoreflect.MessageType) bool {
		if string(messageType.Descriptor().Name()) == *topic {
			mt = messageType
			return false
		}
		return true
	})
	if mt != nil {
		pb := mt.New()
		return pb.Interface(), nil
	}
	return nil, fmt.Errorf("unable to find MessageType for topic: %s", *topic)
}

// DeserializationFunc is a type that describes the function that is ultimately used to
// deserialize a protobuf.
type DeserializationFunc = func([]byte, proto.Message) error

// ProtobufDeserializer hydrates a []byte into a Protobuf which is resolved via
// a ProtobufResolver
type ProtobufDeserializer struct {
	protobufResolver ProtobufResolver
	unmarshal        DeserializationFunc
}

// VTUnmarshal is an inerface satisfied by any protobuf that has been built with
// the protoc-gen-go-vtproto tool to generate an efficient unmarshal method
type VTUnmarshal interface {
	UnmarshalVT(data []byte) error
}

// Unmarshal is a wrapper around proto.Unmarshal which will use UnmarshalVT when
// deserializing any proto that has been modified by protoc-gen-go-vtproto with
// the unmarshal option
func Unmarshal(bytes []byte, msg proto.Message) error {
	switch m := msg.(type) {
	case VTUnmarshal:
		return m.UnmarshalVT(bytes)
	default:
		return proto.Unmarshal(bytes, msg)
	}
}

// NewProtobufDeserializer is a constructor that takes a SchemaRegistryClient
// and a ProtobufResolver, which are used to determine schema and resolve an
// empty protobuf that data can be unmarshalled into.
func NewProtobufDeserializer(
	protobufResolver ProtobufResolver,
	deserializationFunc ...DeserializationFunc,
) *ProtobufDeserializer {
	// marshall via Marshal by default
	unmarshal := Unmarshal
	if len(deserializationFunc) > 0 {
		unmarshal = deserializationFunc[0]
	}

	return &ProtobufDeserializer{
		protobufResolver: protobufResolver,
		unmarshal:        unmarshal,
	}
}

// Deserialize hydrates an []byte into a protobuf instance which is resolved
// from the topic name and schemaId by the ProtobufResolver
func (ps *ProtobufDeserializer) Deserialize(
	topic *string,
	bytes []byte,
) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	}

	bytesRead, schemaId, msgIndexes, err := decodeHeader(topic, bytes)
	if err != nil {
		return nil, err
	}
	// resolve an empty instance of correct protobuf
	pb, err := ps.protobufResolver.ResolveProtobuf(topic, schemaId, msgIndexes)
	if err != nil {
		return nil, err
	}

	// unmarshal into the empty protobuf after the header in bytes
	err = ps.unmarshal(bytes[bytesRead:], pb)
	if err != nil {
		return nil, err
	}
	return pb, nil
}

func decodeHeader(
	topic *string,
	bytes []byte,
) (totalBytesRead int, schemaId uint32, msgIndexes []int64, err error) {

	if bytes[0] != byte(0) {
		err = fmt.Errorf("invalid protobuf wire protocol.  Received version: %v", bytes[0])
		return
	}
	// we should actually validate the schemaId against the topic in some way, but note that it
	// only needs to be compatible with the latest schema, not equal to it.
	schemaId = binary.BigEndian.Uint32(bytes[1:5])

	// decode the number of elements in the array of message indexes
	arrayLen, bytesRead := binary.Varint(bytes[5:])
	if bytesRead <= 0 {
		err = fmt.Errorf("Unable to decode message index array")
		return
	}
	totalBytesRead = 5 + bytesRead
	msgIndexes = make([]int64, arrayLen)
	// iterate arrayLen times, decoding another varint
	for i := int64(0); i < arrayLen; i++ {
		idx, bytesRead := binary.Varint(bytes[totalBytesRead:])
		if bytesRead <= 0 {
			err = fmt.Errorf("unable to decode value in message index array")
			return
		}
		totalBytesRead += bytesRead
		msgIndexes[i] = idx
	}
	return
}

type StringDeserializer struct {
}

func (s *StringDeserializer) Deserialize(topic *string, bytes []byte) (interface{}, error) {
	return string(bytes), nil
}

type JsonDeserializer struct {
	// because json.Unmarshal needs an actual instance to deserialize into,
	// delegate the work to the provided unmarshal function
	unmarshal func(topic *string, data []byte) (interface{}, error)
}

func NewJsonDeserializer(unmarshal func(topic *string, data []byte) (interface{}, error)) *JsonDeserializer {
	return &JsonDeserializer{
		unmarshal: unmarshal,
	}
}

func (s *JsonDeserializer) Deserialize(topic *string, bytes []byte) (interface{}, error) {
	return s.unmarshal(topic, bytes)
}
