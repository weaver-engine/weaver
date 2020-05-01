defmodule Weaver.Util.Record do
  @moduledoc "Provides utility functions to work with `Record`."

  defmacro import_definitions(record_names, opts) do
    quote do
      require Record

      Record.extract_all(unquote(opts))
      |> Keyword.take(unquote(record_names))
      |> Enum.each(fn {record_name, fields} ->
        Record.defrecord(record_name, fields)
      end)
    end
  end
end
