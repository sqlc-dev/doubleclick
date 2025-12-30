package explain

import (
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/ast"
)

// explainDictionaryAttributeDeclaration outputs a dictionary attribute declaration.
func explainDictionaryAttributeDeclaration(sb *strings.Builder, n *ast.DictionaryAttributeDeclaration, indent string, depth int) {
	children := 0
	if n.Type != nil {
		children++
	}
	if n.Default != nil {
		children++
	}
	if n.Expression != nil {
		children++
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sDictionaryAttributeDeclaration %s (children %d)\n", indent, n.Name, children)
	} else {
		fmt.Fprintf(sb, "%sDictionaryAttributeDeclaration %s\n", indent, n.Name)
	}
	if n.Type != nil {
		Node(sb, n.Type, depth+1)
	}
	if n.Default != nil {
		Node(sb, n.Default, depth+1)
	}
	if n.Expression != nil {
		Node(sb, n.Expression, depth+1)
	}
}

// explainDictionaryDefinition outputs a dictionary definition section.
func explainDictionaryDefinition(sb *strings.Builder, n *ast.DictionaryDefinition, indent string, depth int) {
	children := 0
	if len(n.PrimaryKey) > 0 {
		children++
	}
	if n.Source != nil {
		children++
	}
	if n.Lifetime != nil {
		children++
	}
	if n.Layout != nil {
		children++
	}
	if n.Range != nil {
		children++
	}
	if len(n.Settings) > 0 {
		children++
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sDictionary definition (children %d)\n", indent, children)
	} else {
		fmt.Fprintf(sb, "%sDictionary definition\n", indent)
	}

	// PRIMARY KEY
	if len(n.PrimaryKey) > 0 {
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.PrimaryKey))
		for _, pk := range n.PrimaryKey {
			Node(sb, pk, depth+2)
		}
	}

	// SOURCE
	if n.Source != nil {
		explainDictionarySource(sb, n.Source, indent+" ", depth+1)
	}

	// LIFETIME
	if n.Lifetime != nil {
		explainDictionaryLifetime(sb, n.Lifetime, indent+" ", depth+1)
	}

	// RANGE (if present, comes before LAYOUT)
	if n.Range != nil {
		explainDictionaryRange(sb, n.Range, indent+" ", depth+1)
	}

	// LAYOUT
	if n.Layout != nil {
		explainDictionaryLayout(sb, n.Layout, indent+" ", depth+1)
	}

	// SETTINGS
	if len(n.Settings) > 0 {
		fmt.Fprintf(sb, "%s Set\n", indent)
	}
}

// explainDictionarySource outputs a dictionary SOURCE clause.
func explainDictionarySource(sb *strings.Builder, n *ast.DictionarySource, indent string, depth int) {
	// FunctionWithKeyValueArguments has extra space before name
	children := 0
	if len(n.Args) > 0 {
		children = 1
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sFunctionWithKeyValueArguments  %s (children %d)\n", indent, strings.ToLower(n.Type), children)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Args))
		for _, arg := range n.Args {
			explainKeyValuePair(sb, arg, indent+"  ", depth+2)
		}
	} else {
		fmt.Fprintf(sb, "%sFunctionWithKeyValueArguments  %s\n", indent, strings.ToLower(n.Type))
	}
}

// explainKeyValuePair outputs a key-value pair (lowercase "pair").
func explainKeyValuePair(sb *strings.Builder, n *ast.KeyValuePair, indent string, depth int) {
	children := 0
	if n.Value != nil {
		children = 1
	}
	if children > 0 {
		fmt.Fprintf(sb, "%spair (children %d)\n", indent, children)
		Node(sb, n.Value, depth+1)
	} else {
		fmt.Fprintf(sb, "%spair\n", indent)
	}
}

// explainDictionaryLifetime outputs a dictionary LIFETIME clause.
func explainDictionaryLifetime(sb *strings.Builder, n *ast.DictionaryLifetime, indent string, depth int) {
	// LIFETIME is output as "Dictionary lifetime" without children count typically
	fmt.Fprintf(sb, "%sDictionary lifetime\n", indent)
}

// explainDictionaryLayout outputs a dictionary LAYOUT clause.
func explainDictionaryLayout(sb *strings.Builder, n *ast.DictionaryLayout, indent string, depth int) {
	children := 0
	if len(n.Args) > 0 {
		children = 1
	}
	if children > 0 {
		fmt.Fprintf(sb, "%sDictionary layout (children %d)\n", indent, children)
		fmt.Fprintf(sb, "%s ExpressionList (children %d)\n", indent, len(n.Args))
		for _, arg := range n.Args {
			explainKeyValuePair(sb, arg, indent+"  ", depth+2)
		}
	} else {
		fmt.Fprintf(sb, "%sDictionary layout (children 1)\n", indent)
		fmt.Fprintf(sb, "%s ExpressionList\n", indent)
	}
}

// explainDictionaryRange outputs a dictionary RANGE clause.
// Note: ClickHouse's EXPLAIN does not output children for Dictionary range.
func explainDictionaryRange(sb *strings.Builder, n *ast.DictionaryRange, indent string, depth int) {
	fmt.Fprintf(sb, "%sDictionary range\n", indent)
}
