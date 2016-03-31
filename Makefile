default:	test

test:	*.go
	go test -v ./...

fmt:
	gofmt -w .
