using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace TsqlAstParser;

class Program
{
    static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Usage: TsqlAstParser <sql-file> [output-file]");
            Console.Error.WriteLine("       TsqlAstParser --stdin [output-file]");
            Console.Error.WriteLine();
            Console.Error.WriteLine("Parses T-SQL and outputs the AST as JSON.");
            Console.Error.WriteLine();
            Console.Error.WriteLine("Options:");
            Console.Error.WriteLine("  <sql-file>     Path to a .sql file to parse");
            Console.Error.WriteLine("  --stdin        Read SQL from standard input");
            Console.Error.WriteLine("  [output-file]  Optional output file (default: stdout)");
            return 1;
        }

        string sql;
        string? outputPath = null;

        if (args[0] == "--stdin")
        {
            sql = Console.In.ReadToEnd();
            if (args.Length > 1)
            {
                outputPath = args[1];
            }
        }
        else
        {
            var inputPath = args[0];
            if (!File.Exists(inputPath))
            {
                Console.Error.WriteLine($"Error: File not found: {inputPath}");
                return 1;
            }
            sql = File.ReadAllText(inputPath);
            if (args.Length > 1)
            {
                outputPath = args[1];
            }
        }

        try
        {
            var result = ParseSql(sql);
            var jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            };

            var json = JsonSerializer.Serialize(result, jsonOptions);

            if (outputPath != null)
            {
                File.WriteAllText(outputPath, json);
                Console.Error.WriteLine($"AST written to: {outputPath}");
            }
            else
            {
                Console.WriteLine(json);
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    static AstResult ParseSql(string sql)
    {
        // Use TSql160Parser for SQL Server 2022 compatibility (latest)
        var parser = new TSql160Parser(initialQuotedIdentifiers: true);

        using var reader = new StringReader(sql);
        var fragment = parser.Parse(reader, out var errors);

        if (errors.Count > 0)
        {
            return new AstResult
            {
                Errors = errors.Select(e => new ParseErrorInfo
                {
                    Line = e.Line,
                    Column = e.Column,
                    Message = e.Message,
                    Number = e.Number
                }).ToList()
            };
        }

        var converter = new AstConverter();
        var ast = converter.Convert(fragment);

        return new AstResult
        {
            Ast = ast
        };
    }
}

public class AstResult
{
    public object? Ast { get; set; }
    public List<ParseErrorInfo>? Errors { get; set; }
}

public class ParseErrorInfo
{
    public int Line { get; set; }
    public int Column { get; set; }
    public string? Message { get; set; }
    public int Number { get; set; }
}
