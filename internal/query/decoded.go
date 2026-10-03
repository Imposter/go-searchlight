package query

// Decoded returns the condition's value decoded, its texts normalized, as the matcher
// reads it: Arg when [Parse] filled it, else Value decoded (a Leaf built by hand). Its
// lists and object are shared with the leaf: read them, never change them.
func (l *Leaf) Decoded() Arg { return l.arg() }
