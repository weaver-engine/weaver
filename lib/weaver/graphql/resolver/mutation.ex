defmodule Weaver.GraphQL.Resolver.Mutation do
  @moduledoc """
  A placeholder Mutation implementation of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.
  This is not used as Weaver does not handle mutations.

  Also see the (Root setup)[https://shopgun.github.io/graphql-erlang-tutorial/#_root_setup]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def execute(%{op_type: :mutation}, _obj, "createTShirt", %{"input" => _args}) do
    {:ok, %{}}
  end
end
