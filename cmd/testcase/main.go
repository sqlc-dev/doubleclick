package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/sqlc-dev/doubleclick/parser"
)

func main() {
	query := `SELECT "number", CASE "number"
         WHEN 3 THEN 55
         WHEN 6 THEN 77
         WHEN 9 THEN 95
         ELSE CASE
         WHEN "number"=1 THEN 10
         WHEN "number"=10 THEN 100
         ELSE 555555
         END
         END AS "LONG_COL_0"
        FROM ` + "`system`" + `.numbers
        LIMIT 20;`

	stmts, err := parser.Parse(context.Background(), strings.NewReader(query))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if len(stmts) > 0 {
		fmt.Println(parser.Explain(stmts[0]))
	}
}
