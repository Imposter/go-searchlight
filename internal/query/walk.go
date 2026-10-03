package query

// Walk calls fn for n and every node below it, depth first and left to right, with each
// node's path: its loc from [RootLoc] (query.all.2.any.0). When fn returns false, Walk
// does not descend into that node's children, and goes on with its next sibling. Nil
// nodes are skipped.
func Walk(n Node, fn func(path string, n Node) bool) {
	walk(n, RootLoc, fn)
}

func walk(n Node, path string, fn func(string, Node) bool) {
	if isNil(n) || !fn(path, n) {
		return
	}
	switch x := n.(type) {
	case *All:
		at := join(path, "all")
		for i, child := range x.Children {
			walk(child, index(at, i), fn)
		}
	case *Any:
		at := join(path, "any")
		for i, child := range x.Children {
			walk(child, index(at, i), fn)
		}
	case *Not:
		walk(x.Child, join(path, "not"), fn)
	}
}

// isNil reports whether n is nil, or a nil pointer of one of the node types.
func isNil(n Node) bool {
	switch x := n.(type) {
	case nil:
		return true
	case *All:
		return x == nil
	case *Any:
		return x == nil
	case *Not:
		return x == nil
	case *Leaf:
		return x == nil
	default:
		return false
	}
}
