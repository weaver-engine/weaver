defmodule Weaver.Util.Map do
  @moduledoc "Provides utility functions to work with maps."

  @doc """
  Merges value pairs into the given map, deleting all pairs with nil values.

  ## Examples

      iex> Weaver.Util.Map.merge_delete_nil(%{a: 1, b: 2, c: 3}, %{b: 3, c: nil, d: 5, e: nil})
      %{a: 1, b: 3, d: 5}

      iex> Weaver.Util.Map.merge_delete_nil(%{a: 1, b: 2, c: 3}, b: 3, c: nil, d: 5, e: nil)
      %{a: 1, b: 3, d: 5}
  """
  def merge_delete_nil(map, pairs) when is_map(map) do
    Enum.reduce(pairs, map, fn
      {key, nil}, map -> Map.delete(map, key)
      {key, value}, map -> Map.put(map, key, value)
    end)
  end
end
