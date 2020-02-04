defmodule Weaver.GraphQL.Union.Default do
  @moduledoc """
  Default Union implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.

  Also see the (Mapping rules)[https://shopgun.github.io/graphql-erlang-tutorial/#schema-mapping-rules]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def execute(otherwise) do
    {:error, {:unknown_type, otherwise}}
  end
end
