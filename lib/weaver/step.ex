defmodule Weaver.Step do
  @moduledoc """
  Core module handling steps.

  Represents a node in a request, including its (sub)tree of the request.
  Also holds operational meta data about the query.

  Used to pass to Weaver as the main unit of work.
  """

  @enforce_keys [
    :ast
  ]

  defstruct @enforce_keys ++
              [
                :cache,
                :data,
                :uid,
                :fun_env,
                :operation,
                :variables,
                :prev_chunk_end,
                next_chunk_start: :not_loaded,
                refresh: true,
                backfill: true,
                refreshed: false,
                count: 0
              ]

  @type t() :: %__MODULE__{
          ast: tuple(),
          cache: module() | {module(), Keyword.t()} | nil,
          data: any(),
          uid: any(),
          fun_env: function(),
          operation: String.t() | nil,
          variables: map(),
          prev_chunk_end: Weaver.Marker.t() | nil | :not_loaded,
          next_chunk_start: Weaver.Marker.t() | nil | :not_loaded,
          refresh: boolean(),
          backfill: boolean(),
          refreshed: boolean(),
          count: non_neg_integer()
        }

  alias Weaver.{Marker, Ref, Resolvers, Step, Step.Result}

  @doc """
  Takes a step or a list of steps to be processed.

  Returns a tuple of
  - the data, represented as a list of tuples
  - meta data, represented as a list of tuples
  - a list of dispatched steps that should be processed on the
  next level of the graph (may be an empty list)
  - a step to be processed next on the same level of the graph (may be nil)
  """
  @spec process(Step.t()) :: Result.t()
  def process(step) do
    do_process(step, Result.empty())
  end

  def do_process(steps, result) when is_list(steps) do
    Enum.reduce(steps, result, &do_process/2)
  end

  def do_process(step = %Step{ast: {:document, ops}}, result) do
    continue_with(result, step, ops)
  end

  def do_process(
        step = %Step{ast: {:op, _type, _name, _vars, [], fields, _schema_info}},
        result
      ) do
    continue_with(result, step, fields)
  end

  def do_process(
        step = %Step{ast: {:frag, :..., {:name, _, _type}, [], fields, _schema_info}},
        result
      ) do
    continue_with(result, step, fields)
  end

  def do_process(
        step = %Step{
          ast: {:field, {:name, _, "node"}, [{"id", %{value: id}}], _, fields, _, _schema_info}
        },
        result
      ) do
    obj = Resolvers.retrieve_by_id(id)
    ref = Ref.new(id)

    result
    |> Result.add_data({ref, :id, id})
    |> continue_with(obj, step, fields)
  end

  def do_process(
        %Step{ast: {:field, {:name, _, "id"}, [], [], [], :undefined, _schema_info}},
        result
      ) do
    result
  end

  def do_process(
        %Step{
          ast: {:field, {:name, _, field}, [], [], [], :undefined, _schema_info},
          data: parent_obj
        },
        result
      ) do
    value = Resolvers.resolve_leaf(parent_obj, field)

    result
    |> Result.add_data({Ref.from(parent_obj), field, value})
  end

  def do_process(
        step = %Step{
          ast: {:field, {:name, _, field}, [], [], fields, :undefined, _schema_info},
          data: parent_obj
        },
        result
      ) do
    # with total_count = Resolvers.total_count(parent_obj, field),
    #      count = Weaver.Graph.count!(Resolvers.id_for(parent_obj), field),
    #      count == total_count do
    #       Weaver.Graph.stream(Resolvers.id_for(parent_obj), field)
    case Resolvers.resolve_node(parent_obj, field) do
      :dispatch ->
        step = %{step | ast: {:dispatched, step.ast}}
        Result.dispatch(result, step)

      obj ->
        result
        |> Result.add_data({Ref.from(parent_obj), field, Ref.from(obj)})
        |> continue_with(obj, step, fields)
    end
  end

  def do_process(
        step = %Step{
          ast:
            {:dispatched, {:field, {:name, _, field}, [], [], _fields, :undefined, _schema_info}},
          data: parent_obj,
          next_chunk_start: :not_loaded
        },
        result
      ) do
    parent_ref = Ref.from(parent_obj)

    cond do
      step.refresh && !step.refreshed ->
        next_chunk_start =
          markers!(step.cache, parent_ref, field, limit: 1)
          |> List.first()

        step = %{step | next_chunk_start: next_chunk_start}

        do_process(step, result)

      step.backfill ->
        markers!(step.cache, parent_ref, field, limit: 3)
        |> Enum.split_while(&(&1.type != :chunk_end))
        |> case do
          {_refresh_end, [prev_chunk_end | rest]} ->
            step = %{step | prev_chunk_end: prev_chunk_end, next_chunk_start: List.first(rest)}

            do_process(step, result)

          _else ->
            Result.empty()
        end

      true ->
        Result.empty()
    end
  end

  def do_process(
        step = %Step{
          ast:
            {:dispatched, {:field, {:name, _, field}, [], [], fields, :undefined, _schema_info}},
          data: parent_obj
        },
        result
      ) do
    parent_ref = Ref.from(parent_obj)

    resolved = Resolvers.dispatched(parent_obj, field, step.prev_chunk_end)

    {objs, meta, next} =
      case analyze_resolved(resolved, step) do
        {:entire_data, []} ->
          meta = meta_delete_all(step, parent_ref, field)

          {[], meta, nil}

        {:entire_data, objs} ->
          meta =
            [{:add, parent_ref, field, Resolvers.start_marker(objs)}] ++
              meta_delete_all(step, parent_ref, field)

          {objs, meta, nil}

        {:last_data, objs} ->
          meta =
            [{:del, parent_ref, field, step.prev_chunk_end}] ++
              meta_delete_all(step, parent_ref, field, less_than: step.prev_chunk_end.val)

          {objs, meta, nil}

        # no gap or gap not closed -> continue with this marker
        {:continue, objs, new_chunk_end} ->
          meta = [
            first_meta(step, resolved, parent_ref, field),
            {:add, parent_ref, field, new_chunk_end}
          ]

          next = %{step | prev_chunk_end: new_chunk_end, count: step.count + length(objs)}

          {objs, meta, next}

        # gap closed -> look up the next chunk start in next iteration
        {:gap_closed, objs} ->
          meta = [
            first_meta(step, resolved, parent_ref, field),
            {:del, parent_ref, field, step.next_chunk_start}
          ]

          next = %{
            step
            | prev_chunk_end: :not_loaded,
              next_chunk_start: :not_loaded,
              refreshed: true,
              count: step.count + length(objs)
          }

          {objs, meta, next}
      end

    result
    |> Result.add_relation_data({parent_ref, field, objs})
    |> Result.add_meta(meta)
    |> Result.set_next(next)
    |> continue_with(objs, step, fields)
  end

  def do_process(step, _next) do
    raise "Undhandled step:\n\n#{inspect(Map.from_struct(step), pretty: true)}"
  end

  defp first_meta(step = %{prev_chunk_end: %Marker{}}, _resolved, parent_ref, field) do
    {:del, parent_ref, field, step.prev_chunk_end}
  end

  defp first_meta(_step, {:continue, objs, _marker}, parent_ref, field) do
    {:add, parent_ref, field, Resolvers.start_marker(objs)}
  end

  defp meta_delete_all(step, parent_ref, field, opts \\ []) do
    step.cache
    |> markers!(parent_ref, field, opts)
    |> Enum.map(&{:del, parent_ref, field, &1})
  end

  defp analyze_resolved({:done, objs}, %Step{prev_chunk_end: %Marker{}}) do
    {:last_data, objs}
  end

  defp analyze_resolved({:done, objs}, _) do
    {:entire_data, objs}
  end

  # no gap
  defp analyze_resolved({:continue, objs, new_chunk_end}, %Step{next_chunk_start: nil}) do
    {:continue, objs, new_chunk_end}
  end

  # gap closed?
  defp analyze_resolved({:continue, objs, new_chunk_end}, step = %Step{}) do
    case Enum.split_while(objs, &before_marker?(&1, step.next_chunk_start)) do
      {objs, []} -> {:continue, objs, new_chunk_end}
      {objs, __} -> {:gap_closed, objs}
    end
  end

  defp before_marker?(obj, marker) do
    Resolvers.marker_val(obj) > marker.val &&
      Resolvers.id_for(obj) != marker.ref.id
  end

  defp continue_with(result, step, subtree) do
    for elem <- subtree do
      %{step | ast: elem}
    end
    |> do_process(result)
  end

  defp continue_with(result, objs, step, subtree) when is_list(objs) do
    for obj <- objs, elem <- subtree do
      %{step | data: obj, ast: elem}
    end
    |> do_process(result)
  end

  defp continue_with(result, obj, step, subtree) do
    new_step = Map.put(step, :data, obj)
    continue_with(result, new_step, subtree)
  end

  defp markers!(nil, _parent_ref, _field, _opts), do: []

  defp markers!({mod, cache_opts}, parent_ref, field, opts) do
    mod.markers!(parent_ref, field, Keyword.merge(cache_opts, opts))
  end

  defp markers!(mod, parent_ref, field, opts) do
    mod.markers!(parent_ref, field, opts)
  end
end
