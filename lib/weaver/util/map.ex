defmodule Weaver.Util.Map do
  @moduledoc "Provides utility functions to work with maps."

  @doc """
  Merges all non-nil value pairs into the given map

  ## Examples

      iex> Weaver.Util.Map.merge_non_nil(%{a: 1, b: 2}, %{b: 3, c: 4, e: nil})
      %{a: 1, b: 3, c: 4}

      iex> Weaver.Util.Map.merge_non_nil(%{a: 1, b: 2}, b: 3, c: 4, e: nil)
      %{a: 1, b: 3, c: 4}
  """
  def merge_non_nil(map, pairs) when is_map(map) do
    Enum.reduce(pairs, map, fn
      {_key, nil}, map -> map
      {key, value}, map -> Map.put(map, key, value)
    end)
  end
end
