defmodule Weaver.Absinthe.Middleware.Dispatch do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  @behaviour Absinthe.Middleware

  def call(%{state: :unresolved} = res, fun) when is_function(fun) do
    %{
      res
      | state: :suspended,
        acc: Map.put(res.acc, __MODULE__, true)
    }
  end
end
