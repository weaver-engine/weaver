defmodule Weaver.Absinthe.Middleware.Continue do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  @behaviour Absinthe.Middleware

  # call resolver function only if this is the resolution part for the current step
  def call(%{state: :suspended, acc: %{resolution: path}, path: path} = res, fun) do
    case fun.(res.acc[:please_come_again]) do
      {:continue, value, end_marker} ->
        %{
          res
          | acc:
              res.acc
              |> Map.put(__MODULE__, res.path)
              |> Map.put(:please_come_again, end_marker),
            # context: Map.put(res.context, :end_marker, end_marker),
            middleware: [{__MODULE__, fun} | res.middleware]
            # middleware: [{__MODULE__, {fun, end_marker}} | res.middleware]
        }
        |> Absinthe.Resolution.put_result({:ok, value})

      {:done, value} ->
        %{
          res
          | acc: res.acc |> Map.delete(__MODULE__) |> Map.delete(:please_come_again),
            context: Map.delete(res.context, :end_marker)
        }
        |> Absinthe.Resolution.put_result({:ok, value})
    end
  end

  # ... skip otherwise
  def call(res, _fun) do
    res
  end
end
