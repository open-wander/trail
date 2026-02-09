.PHONY: build test lint clean run

APP     := trail
BUILD   := ./cmd/trail
OUT     := ./bin/$(APP)

build:
	go build -o $(OUT) $(BUILD)

test:
	go test ./...

lint:
	go vet ./...

clean:
	rm -f $(OUT)

run: build
	$(OUT)
