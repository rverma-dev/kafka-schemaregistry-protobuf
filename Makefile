proto:
	protoc -Ipb --go_out=. pb/orders.proto

# FIXME: Need dynamic tag only for apple M1
build: 
	CGO_ENABLED=1 go build -tags dynamic

default: proto build