# parquet

Refactoring of https://github.com/xitongsys/parquet-go (Copyright 2017 Xitong Zhang)

- Code was linted and refactored using golangci-lint
- Added a couple of missing error checking
- Relevant sub packages were moved into root pkg
- Some of the packages related to internals were made private
- JSON marshalling removed (feels like it should be elsewhere)

## Planned

- refactor tagging system to a more strong typed approach and ditch the custom
  tag dsl
- improve error handling
- use io.Writer and at most io.ReadSeeker

## Status

It's the same code base but instead of using sub packages, it should work as:

```go
import (
	"github.com/xintongsys/parquet-go-source/local"
	"github.com/stdiopt/parquet"
)

type My struct {
	metric1 int `parquet:"name=metric1"`
	metric2 int `parquet:"name=metric2"`
}

func main() {
	f, err := local.NewLocalFileWriter("name.parquet")
	// err check
	defer f.Close()

	pw, err := parquet.NewWriter(f, &My{},1)
	// err check
	defer pw.Close()

	err := pw.Write(&My{1,2})
	// errcheck
}
```
