package lib

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TODO: protobuf registry registers globaltypes with filename whereas schemaRegistry stores by Int32
var subjectMap = map[string]string{
	"100003": "orders",
}

// SchemaRegistryProtobufResolver
type SchemaRegistryProtobufResolver struct {
	schemaRegistry      srclient.SchemaRegistryClient
	protobufRegistry    ProtobufRegistry
	deserializationType DeserializationType
}

// NewSchemaRegistryProtobufResolver
func NewSchemaRegistryProtobufResolver(
	schemaRegistry srclient.SchemaRegistryClient,
	protobufRegistry ProtobufRegistry,
	deserializationType DeserializationType,
) *SchemaRegistryProtobufResolver {
	return &SchemaRegistryProtobufResolver{
		schemaRegistry:      schemaRegistry,
		protobufRegistry:    protobufRegistry,
		deserializationType: deserializationType,
	}
}

// This should probably exist in srclient
func (reg *SchemaRegistryProtobufResolver) parseSchema(schemaId int) (*desc.FileDescriptor, error) {
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema *srclient.Schema
			var err error

			// filename is a schema id, fetch it directly
			if schemaId, err = strconv.Atoi(filename); err == nil {
				schema, err = reg.schemaRegistry.GetSchema(schemaId)
			} else {
				// otherwise its likely an import and we look it up by its filename
				schema, err = reg.schemaRegistry.GetLatestSchema(filename)
			}

			if err != nil {
				return nil, err
			}
			if *(schema.SchemaType()) != srclient.Protobuf {
				return nil, fmt.Errorf("schema %v is not a Protobuf schema", schemaId)
			}
			return io.NopCloser(strings.NewReader(schema.Schema())), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(strconv.Itoa(schemaId))
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("unexpected schema from schema registry")
	}
	return fileDescriptors[0], nil
}

// ResolveProtobuf
func (reg *SchemaRegistryProtobufResolver) ResolveProtobuf(
	topic *string,
	schemaId uint32,
	msgIndexes []int64,
) (proto.Message, error) {

	fileDescriptor, err := reg.parseSchema(int(schemaId))
	if err != nil {
		return nil, err
	}

	msg := resolveDescriptorByIndexes(msgIndexes, fileDescriptor)

	var mt protoreflect.MessageType
	reg.protobufRegistry.RangeMessages(func(messageType protoreflect.MessageType) bool {
		if string(messageType.Descriptor().Name()) == subjectMap[msg.GetName()] {
			mt = messageType
			return false
		}
		return true
	})
	if mt != nil {
		pb := mt.New()
		return pb.Interface(), nil
	}
	return nil, fmt.Errorf("unable to find MessageType for messageIndex %v inside schema %v", msgIndexes, schemaId)
}

func resolveDescriptorByIndexes(msgIndexes []int64, descriptor desc.Descriptor) desc.Descriptor {
	if len(msgIndexes) == 0 {
		return descriptor
	}

	index := msgIndexes[0]
	msgIndexes = msgIndexes[1:]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		return resolveDescriptorByIndexes(msgIndexes, v.GetMessageTypes()[index])
	case *desc.MessageDescriptor:
		if len(msgIndexes) > 0 {
			return resolveDescriptorByIndexes(msgIndexes, v.GetNestedMessageTypes()[index])
		} else {
			return v.GetNestedMessageTypes()[index]
		}
	default:
		fmt.Printf("no match: %v\n", v)
		return nil
	}
}
