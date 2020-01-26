defmodule Weaver.GraphQL.Interface.Default do
  @moduledoc """
  Default Interface implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.

  Also see the (Mapping rules)[https://shopgun.github.io/graphql-erlang-tutorial/#schema-mapping-rules]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  @doc """
  Tries to determine the GraphQL type for any given object struct.
  """
  def execute(%{:__struct__ => module}) do
    type =
      if function_exported?(module, :graphql_type, 0) do
        module.graphql_type()
      else
        module
        |> to_string()
        |> String.split(".")
        |> Enum.reverse()
        |> Enum.take(2)
        |> Enum.reverse()
        |> Enum.join()
      end

    {:ok, String.to_atom(type)}
  end

  def execute(otherwise) do
    {:error, {:unknown_type, otherwise}}
  end
end
