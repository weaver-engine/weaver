defmodule Weaver.GraphQL.Resolver.QueryRoot do
  @moduledoc """
  A placeholder Query Root implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.
  This is not used as Weaver implements its own resolvers.

  Also see the (Root setup)[https://shopgun.github.io/graphql-erlang-tutorial/#_root_setup]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def execute(_ctx, :none, "node", %{"id" => id}) do
    {:ok, Weaver.Resolvers.retrieve_by_id(id)}
  end
end
