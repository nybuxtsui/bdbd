CGO_CXXFLAGS="-I$HOME/local/bdb/include" CGO_CFLAGS="-I$HOME/local/bdb/include" CGO_LDFLAGS="-L$HOME/local/bdb/lib" go install github.com/nybuxtsui/bdbd/bdb
CGO_CXXFLAGS="-I$HOME/local/bdb/include" CGO_CFLAGS="-I$HOME/local/bdb/include" CGO_LDFLAGS="-L$HOME/local/bdb/lib" go run bdbd/main.go
