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
    order =
      res.definition.schema_node
      |> Absinthe.Type.meta()
      |> Map.take([:ordered_by, :order, :unique])

    type_name =
      res.definition.schema_node.type
      |> Absinthe.Type.unwrap()
      |> Absinthe.Type.expand(res.schema)
      |> Map.get(:name)

    id_fun =
      res.definition.schema_node.type
      |> Absinthe.Type.unwrap()
      |> Absinthe.Type.expand(res.schema)
      |> id_fun_for()

    id_for = fn obj -> "#{type_name}:#{id_fun.(obj)}" end

    cache = res.context.cache

    parent_type_name = Map.get(res.parent_type, :name)
    parent_id_fun = id_fun_for(res.parent_type)

    parent_ref = res.source && Ref.new("#{parent_type_name}:#{parent_id_fun.(res.source)}")
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
        prev_cursor = step.prev_chunk_end && step.prev_chunk_end.cursor
        resolved = fun.(prev_cursor)

        {value, meta, next} =
          Step.process_resolved(resolved, step, cache, parent_ref, field, order, id_for)

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

  def id_fun_for(schema_type) do
    schema_type
    |> Absinthe.Type.meta(:weaver_id)
    |> case do
      nil -> &Weaver.Node.id_for/1
      fun when is_function(fun) -> fun
      key -> &Map.get(&1, key)
    end
  end
end
