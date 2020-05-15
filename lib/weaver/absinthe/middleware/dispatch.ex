defmodule Weaver.Absinthe.Middleware.Dispatch do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  @behaviour Absinthe.Middleware

  def call(%{state: :unresolved} = res, fun) when is_function(fun) do
    list = Map.get(res.acc, __MODULE__, [])

    %{
      res
      | state: :suspended,
        acc: Map.put(res.acc, __MODULE__, [res.path | list]),
        middleware: [{__MODULE__, fun} | res.middleware]
    }
  end

  def call(%{state: :suspended} = res, fun) do
    result = fun.()

    res
    |> Absinthe.Resolution.put_result(result)
  end
end
