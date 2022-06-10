proto:
	protoc -Ipb --go_out=. pb/orders.proto

build: proto
	go build ./...