package multierror

import "bytes"

// MultipleErrors is an implementation of error that is made of other errors.
// This is useful in situations where it makes sense to collect and present as
// many errors as possible, e.g. in validating user input, rather than to stop
// at the first error and report it.
type MultipleErrors []error

func (e MultipleErrors) Error() string {
	var b bytes.Buffer
	b.WriteString("encountered multiple errors:")
	for _, child := range e {
		b.WriteString("\n\t... " + child.Error())
	}
	return b.String()
}

// New returns a (possibly composite) error that represents the errors in the
// provided list.
//
// Any nil errors in the list are removed, and any MultipleErrors instances in
// the list are replaced with their inlined contents (recursively).  If the
// resulting list is empty, error(nil) is returned.  If the resulting list
// contains a single error, that error is returned directly.  Otherwise, an
// instance of MultipleErrors is returned.
func New(errors []error) error {
	input := errors
	output := make([]error, 0, len(input))
	for len(input) > 0 {
		e := input[0]
		if me, ok := e.(MultipleErrors); ok {
			tmp := make([]error, len(me)+len(input)-1)
			copy(tmp[0:len(me)], me)
			copy(tmp[len(me):], input[1:])
			input = tmp
			continue
		}
		if e != nil {
			output = append(output, e)
		}
		input = input[1:]
	}
	if len(output) == 0 {
		return nil
	}
	if len(output) == 1 {
		return output[0]
	}
	return MultipleErrors(output)
}

// Of is syntactic sugar; it simply calls New with the given errors.
func Of(errors ...error) error {
	return New(errors)
}
