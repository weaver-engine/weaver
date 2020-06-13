defmodule Weaver.Absinthe.Middleware.Dispatch do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  @behaviour Absinthe.Middleware

  alias Weaver.Absinthe.Middleware.Continue

  def call(%{state: :unresolved} = res, fun) when is_function(fun) do
    list = Map.get(res.acc, __MODULE__, [])

    %{
      res
      | state: :suspended,
        acc: Map.put(res.acc, __MODULE__, [res.path | list]),
        middleware: [{Continue, fun} | res.middleware]
    }
  end
end
