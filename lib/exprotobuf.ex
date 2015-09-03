defmodule Protobuf do
  alias Protobuf.Parser
  alias Protobuf.Builder
  alias Protobuf.Config
  alias Protobuf.ConfigError
  alias Protobuf.Field
  alias Protobuf.Utils

  defmacro __using__(opts) do
    namespace = __CALLER__.module

    opts = opts |> Enum.into %{}
    files = Path.wildcard(elem(Code.eval_quoted(opts[:from]), 0))

    config = %Config{namespace: __CALLER__.module, schema: nil, inject: true}
    results = Enum.flat_map files, fn (file) ->
      schema = File.read!(file)
      config = %Config{namespace: __CALLER__.module, schema: schema, inject: true, from_file: file}
      config |> parse(__CALLER__)
    end

    results |> Enum.uniq |> Builder.define(config)
  end

  # Parse and fix namespaces of parsed types
  defp parse(%Config{namespace: ns, schema: schema, inject: inject, from_file: nil}, _) do
    Parser.parse!(schema) |> namespace_types(ns, inject)
  end
  defp parse(%Config{namespace: ns, schema: schema, inject: inject, from_file: file}, caller) do
    {path, _} = Code.eval_quoted(file, [], caller)
    path      = Path.expand(path) |> Path.dirname
    opts      = [imports: [path], use_packages: true]
    Parser.parse!(schema, opts) |> namespace_types(ns, inject)
  end

  # Find the package namespace
  defp detect_package(parsed) do
    parsed |> Enum.find_value(fn(row) ->
      case row do
        {:package, package} -> package
        _ -> false
      end
    end)
  end

  defp namespace_types(parsed, ns, inject) do
    # Apply namespace to top-level types
    detect_package(parsed) |> namespace_types(parsed, ns, inject)
  end

  # Apply namespace to top-level types
  defp namespace_types(package, parsed, ns, inject) do
    for {{type, name}, fields} <- parsed do
      if inject do
        if false && package do
          {{type, :"Elixir.#{package}.#{name |> normalize_name}"}, namespace_fields(type, fields, ns)}
        else
          {{type, :"Elixir.#{name |> normalize_name}"}, namespace_fields(type, fields, ns)}
        end
      else
        if false && package do
          {{type, :"#{ns}.#{package}.#{name |> normalize_name}"}, namespace_fields(type, fields, ns)}
        else
          {{type, :"#{ns}.#{name |> normalize_name}"}, namespace_fields(type, fields, ns)}
        end
      end
    end
  end

  # Apply namespace to nested types
  defp namespace_fields(:msg, fields, ns), do: Enum.map(fields, &namespace_fields(&1, ns))
  defp namespace_fields(_, fields, _),     do: fields
  defp namespace_fields(field, ns) when not is_map(field) do
    field |> Utils.convert_from_record(Field) |> namespace_fields(ns)
  end
  defp namespace_fields(%Field{type: {type, name}} = field, ns) do
    %{field | :type => {type, :"#{ns}.#{name |> normalize_name}"}}
  end
  defp namespace_fields(%Field{} = field, _ns) do
    field
  end

  # Normalizes module names by ensuring they are cased correctly
  # (respects camel-case and nested modules)
  defp normalize_name(name) do
    name
    |> Atom.to_string
    |> String.split(".", parts: :infinity)
    |> Enum.map(fn(x) -> String.split_at(x, 1) end)
    |> Enum.map(fn({first, remainder}) -> String.upcase(first) <> remainder end)
    |> Enum.join(".")
    |> String.to_atom
  end
end
