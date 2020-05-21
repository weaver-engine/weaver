defmodule Weaver.Absinthe.Middleware.Continue do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  defstruct [
    :prev_chunk_end
  ]

  @type t() :: %__MODULE__{
          prev_chunk_end: Weaver.Marker.t() | nil | :not_loaded
        }

  @behaviour Absinthe.Middleware

  # call resolver function only if this is the resolution part for the current step
  def call(%{state: :suspended, acc: %{resolution: path}, path: path} = res, fun) do
    acc = Map.get(res.acc, __MODULE__, %__MODULE__{})

    case fun.(acc.prev_chunk_end) do
      {:continue, value, end_marker} ->
        new_acc = %{acc | prev_chunk_end: end_marker}

        %{
          res
          | acc: Map.put(res.acc, __MODULE__, new_acc),
            middleware: [{__MODULE__, fun} | res.middleware]
            # middleware: [{__MODULE__, {fun, end_marker}} | res.middleware]
        }
        |> Absinthe.Resolution.put_result({:ok, value})

      {:done, value} ->
        %{
          res
          | acc: Map.delete(res.acc, __MODULE__)
        }
        |> Absinthe.Resolution.put_result({:ok, value})
    end
  end

  # ... skip otherwise
  def call(res, _fun) do
    res
  end
end
