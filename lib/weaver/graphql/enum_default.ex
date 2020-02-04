defmodule Weaver.GraphQL.Enum.Default do
  @moduledoc """
  Default Enum implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.

  Also see the (Mapping rules)[https://shopgun.github.io/graphql-erlang-tutorial/#schema-mapping-rules]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def output(_default, enum) do
    {:error, {:unknown_enum, enum}}
  end
end
