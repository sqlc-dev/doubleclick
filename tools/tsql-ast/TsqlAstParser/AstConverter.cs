using System.Collections;
using System.Reflection;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace TsqlAstParser;

/// <summary>
/// Converts SqlScriptDOM TSqlFragment objects to JSON-serializable dictionaries.
/// Uses reflection to traverse the AST and extract all properties.
/// </summary>
public class AstConverter
{
    private readonly HashSet<object> _visited = new();

    // Properties to exclude from serialization (internal/position-related)
    private static readonly HashSet<string> ExcludedProperties = new(StringComparer.OrdinalIgnoreCase)
    {
        "StartOffset",
        "FragmentLength",
        "StartLine",
        "StartColumn",
        "FirstTokenIndex",
        "LastTokenIndex",
        "ScriptTokenStream"
    };

    public object? Convert(TSqlFragment? fragment)
    {
        if (fragment == null) return null;

        return ConvertFragment(fragment);
    }

    private object? ConvertFragment(TSqlFragment fragment)
    {
        // Prevent infinite recursion from circular references
        if (!_visited.Add(fragment))
        {
            return new Dictionary<string, object?>
            {
                ["_type"] = fragment.GetType().Name,
                ["_circular_ref"] = true
            };
        }

        var result = new Dictionary<string, object?>
        {
            ["_type"] = fragment.GetType().Name
        };

        // Get all public instance properties
        var properties = fragment.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            if (ExcludedProperties.Contains(prop.Name)) continue;
            if (!prop.CanRead) continue;

            try
            {
                var value = prop.GetValue(fragment);
                var converted = ConvertValue(value, prop.PropertyType);

                if (converted != null || !IsNullableOrReferenceType(prop.PropertyType))
                {
                    // Skip empty collections
                    if (converted is IList list && list.Count == 0) continue;

                    result[prop.Name] = converted;
                }
            }
            catch
            {
                // Skip properties that throw exceptions
            }
        }

        return result;
    }

    private object? ConvertValue(object? value, Type declaredType)
    {
        if (value == null) return null;

        var type = value.GetType();

        // Handle TSqlFragment (AST nodes)
        if (value is TSqlFragment fragment)
        {
            return ConvertFragment(fragment);
        }

        // Handle collections of TSqlFragment
        if (value is IEnumerable enumerable && type != typeof(string))
        {
            var list = new List<object?>();
            foreach (var item in enumerable)
            {
                list.Add(ConvertValue(item, item?.GetType() ?? typeof(object)));
            }
            return list.Count > 0 ? list : null;
        }

        // Handle enums - convert to string
        if (type.IsEnum)
        {
            return value.ToString();
        }

        // Handle primitives and strings
        if (type.IsPrimitive || value is string || value is decimal)
        {
            return value;
        }

        // Handle Identifier specifically for cleaner output
        if (value is Identifier identifier)
        {
            return new Dictionary<string, object?>
            {
                ["_type"] = "Identifier",
                ["Value"] = identifier.Value,
                ["QuoteType"] = identifier.QuoteType.ToString()
            };
        }

        // For other complex types, try to serialize their properties
        return ConvertObject(value);
    }

    private object? ConvertObject(object obj)
    {
        if (!_visited.Add(obj))
        {
            return new Dictionary<string, object?>
            {
                ["_type"] = obj.GetType().Name,
                ["_circular_ref"] = true
            };
        }

        var result = new Dictionary<string, object?>
        {
            ["_type"] = obj.GetType().Name
        };

        var properties = obj.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            if (ExcludedProperties.Contains(prop.Name)) continue;
            if (!prop.CanRead) continue;

            try
            {
                var value = prop.GetValue(obj);
                if (value == null) continue;

                var converted = ConvertValue(value, prop.PropertyType);
                if (converted != null)
                {
                    result[prop.Name] = converted;
                }
            }
            catch
            {
                // Skip properties that throw exceptions
            }
        }

        return result.Count > 1 ? result : null; // Only return if we have more than just _type
    }

    private static bool IsNullableOrReferenceType(Type type)
    {
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }
}
