defmodule Weaver.GraphQL.Scalar.Default do
  @moduledoc """
  Default Scalar implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.

  Also see the (Mapping rules)[https://shopgun.github.io/graphql-erlang-tutorial/#schema-mapping-rules]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def input(_type, value) do
    {:ok, value}
  end

  def output(_type, value) do
    {:ok, value}
  end
end
