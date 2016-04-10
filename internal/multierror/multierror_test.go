package multierror

import "fmt"

type exampleError struct {
	Name string
}

func (err exampleError) Error() string {
	return err.Name
}

var (
	errorA = exampleError{"A"}
	errorB = exampleError{"B"}
	errorC = exampleError{"C"}
)

func ExampleMultipleErrors_empty() {
	fmt.Printf("%T: %[1]v\n", Of())
	// Output:
	// <nil>: <nil>
}

func ExampleMultipleErrors_nil() {
	fmt.Printf("%T: %[1]v\n", Of(nil, nil, nil))
	// Output:
	// <nil>: <nil>
}

func ExampleMultipleErrors_one() {
	fmt.Printf("%T: %[1]v\n", Of(errorA))
	// Output:
	// multierror.exampleError: A
}

func ExampleMultipleErrors_many() {
	fmt.Printf("%T: %[1]v\n", Of(errorA, errorB, errorC))
	// Output:
	// multierror.MultipleErrors: encountered multiple errors:
	//	... A
	//	... B
	//	... C
}

func ExampleMultipleErrors_nested() {
	inner := MultipleErrors{errorA, nil}
	mid := MultipleErrors{inner, nil, errorB}
	outer := Of(mid, errorC)
	fmt.Printf("%T: %[1]v\n", Of(errorA, errorB, errorC))
	fmt.Printf("Len=%d\n", len(outer.(MultipleErrors)))
	// Output:
	// multierror.MultipleErrors: encountered multiple errors:
	//	... A
	//	... B
	//	... C
	// Len=3
}
