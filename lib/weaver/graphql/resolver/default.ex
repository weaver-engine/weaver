defmodule Weaver.GraphQL.Resolver.Default do
  @moduledoc """
  A placeholder Default resolver of the `:graphql_erl` library loaded in `Weaver.load_schema/0`.
  This is not used as Weaver implements its own resolvers.

  Also see the (Default Mapping)[https://shopgun.github.io/graphql-erlang-tutorial/#_default_mapping]
  section of the [Erlang GraphQL Tutorial](https://shopgun.github.io/graphql-erlang-tutorial/).
  """

  def execute(_ctx, obj = %{__struct__: _}, field, _args) do
    try do
      value = Map.get(obj, String.to_existing_atom(field), :null)
      {:ok, value}
    catch
      ArgumentError -> {:error, :null}
    end
  end

  def execute(_ctx, %{"tshirt" => obj}, "tshirt", _args) do
    {:ok, obj}
  end

  def execute(_ctx, _obj, field, _args) do
    {:error, {:unknown_field, field}}
  end
end
