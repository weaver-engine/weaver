defmodule Weaver.GraphQL.Resolver.QueryRoot do
  def execute(_ctx, :none, "node", %{"id" => id}) do
    {:ok, Weaver.Resolvers.retrieve_by_id(id)}
  end
end
