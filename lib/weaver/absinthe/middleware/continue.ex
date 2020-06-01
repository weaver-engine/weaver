defmodule Weaver.Absinthe.Middleware.Continue do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  # credo:disable-for-this-file Credo.Check.Consistency.ParameterPatternMatching

  alias Weaver.{Marker, Ref, Step}

  defstruct [
    :prev_chunk_end,
    next_chunk_start: :not_loaded,
    refreshed: false,
    count: 0
  ]

  @type t() :: %__MODULE__{
          prev_chunk_end: Marker.t() | nil | :not_loaded,
          next_chunk_start: Marker.t() | nil | :not_loaded,
          refreshed: boolean(),
          count: non_neg_integer()
        }

  @behaviour Absinthe.Middleware

  # call resolver function only if this is the resolution part for the current step
  def call(%{state: :suspended, acc: %{resolution: path}, path: path} = res, fun) do
    cache = res.context.cache
    parent_ref = res.source && Ref.from(res.source)
    [%Absinthe.Blueprint.Document.Field{name: field} | _] = path

    Map.get(res.acc, __MODULE__, %__MODULE__{})
    |> Step.load_markers(res.context, cache, parent_ref, field)
    |> case do
      %{prev_chunk_end: :not_loaded} ->
        %{
          res
          | acc: Map.put(res.acc, __MODULE__, nil) |> Map.put(:meta, [])
        }
        |> Absinthe.Resolution.put_result({:ok, []})

      step ->
        resolved = fun.(step.prev_chunk_end)

        {value, meta, next} = Step.process_resolved(resolved, step, cache, parent_ref, field)

        %{
          res
          | acc: Map.put(res.acc, __MODULE__, next) |> Map.put(:meta, meta),
            middleware: [{__MODULE__, fun} | res.middleware]
        }
        |> Absinthe.Resolution.put_result({:ok, value})
    end
  end

  # ... skip otherwise
  def call(res, _fun) do
    res
  end
end
