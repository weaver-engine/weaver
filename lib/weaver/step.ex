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
                :callback,
                :cache,
                :data,
                :uid,
                :fun_env,
                :operation,
                :variables,
                :cursor,
                refresh: true,
                backfill: true,
                refreshed: false,
                gap: :not_loaded,
                count: 0
              ]

  @type t() :: %__MODULE__{
          ast: tuple(),
          callback: function() | nil,
          cache: module() | {module(), Keyword.t()} | nil,
          data: any(),
          uid: any(),
          fun_env: function(),
          operation: String.t() | nil,
          variables: map(),
          cursor: Weaver.Cursor.t() | nil,
          refresh: boolean(),
          backfill: boolean(),
          refreshed: boolean(),
          gap: any(),
          count: non_neg_integer()
        }

  defmodule Result do
    @moduledoc """
    Functions to initialize and modify the 4-element tuple
    returned as the result of `Weaver.Step.process/1`.
    """

    alias Weaver.{Ref, Step}

    @type t() :: {
            list(tuple()),
            list(tuple()),
            list(Step.t()),
            Step.t() | nil
          }

    def empty() do
      {[], [], [], nil}
    end

    def data({data, _, _, _}), do: data
    def meta({_, meta, _, _}), do: meta
    def dispatched({_, _, dispatched, _}), do: dispatched
    def next({_, _, _, next}), do: next

    def add_data({data, meta, dispatched, next}, tuple) do
      {[tuple | data], meta, dispatched, next}
    end

    def add_relation_data(result, {from = %Ref{}, predicate, [obj | objs]}) do
      result
      |> add_data({from, predicate, Ref.from(obj)})
      |> add_relation_data({from, predicate, objs})
    end

    def add_relation_data(result, {%Ref{}, _predicate, []}), do: result

    def add_meta({data, meta, dispatched, next}, tuples) when is_list(tuples) do
      {data, tuples ++ meta, dispatched, next}
    end

    def dispatch({data, meta, dispatched, next}, tuple) do
      {data, meta, [tuple | dispatched], next}
    end

    def set_next({data, meta, dispatched, _next}, step) do
      {data, meta, dispatched, step}
    end
  end

  alias Weaver.{Cursor, Ref, Resolvers, Step, Step.Result}

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
          gap: :not_loaded
        },
        result
      ) do
    parent_ref = Ref.from(parent_obj)

    cond do
      step.refresh && !step.refreshed ->
        gap =
          cursors!(step.cache, parent_ref, field, 1)
          |> List.first()

        step = %{step | gap: gap}

        do_process(step, result)

      step.backfill ->
        cursors!(step.cache, parent_ref, field, 3)
        |> Enum.split_while(&(!&1.gap))
        |> case do
          {_refresh_end, [gap_start | rest]} ->
            step = %{step | cursor: gap_start, gap: List.first(rest)}

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
    resolved = Resolvers.dispatched(parent_obj, field, step.cursor)
    first_meta = first_meta(step, resolved, parent_ref, field)

    {objs, meta, next} =
      case analyze_resolved(resolved, step) do
        {:done, objs} ->
          meta = [
            first_meta
          ]

          {objs, meta, nil}

        # no gap or gap not closed -> continue with this cursor
        {:continue, objs, cursor, gap_closed: false} ->
          cursor = %{cursor | gap: true}

          meta = [
            first_meta,
            {:add, parent_ref, field, cursor}
          ]

          next = %{step | cursor: cursor, count: step.count + length(objs)}
          {objs, meta, next}

        # gap closed and next sequence is only a single record
        # -> continue with the single-entry cursor
        {:continue, objs, gap_closed: true, next_gap: true} ->
          meta = [first_meta]

          next = %{
            step
            | cursor: step.gap,
              gap: :not_loaded,
              refreshed: true,
              count: step.count + length(objs)
          }

          {objs, meta, next}

        # gap closed -> look up the next gap in next iteration
        {:continue, objs, gap_closed: true, next_gap: _} ->
          meta = [
            first_meta,
            {:del, parent_ref, field, step.gap}
          ]

          next = %{
            step
            | gap: :not_loaded,
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

  defp first_meta(step = %{cursor: %Cursor{}}, _resolved, parent_ref, field) do
    {:del, parent_ref, field, step.cursor}
  end

  defp first_meta(_step, {:continue, objs, _cursor}, parent_ref, field) do
    {:add, parent_ref, field, Resolvers.start_cursor(objs)}
  end

  defp first_meta(_step, {:done, objs}, parent_ref, field) do
    {:add, parent_ref, field, Resolvers.start_cursor(objs)}
  end

  defp analyze_resolved({:done, objs}, _) do
    {:done, objs}
  end

  # no gap
  defp analyze_resolved({:continue, objs, cursor}, %{gap: nil}) do
    {:continue, objs, cursor, gap_closed: false}
  end

  # gap closed?
  defp analyze_resolved({:continue, objs, cursor}, %{gap: gap}) do
    case Enum.split_while(objs, &before_cursor?(&1, gap)) do
      {objs, []} -> {:continue, objs, cursor, gap_closed: false}
      {objs, __} -> {:continue, objs, gap_closed: true, next_gap: gap.gap}
    end
  end

  defp before_cursor?(obj, cursor) do
    Resolvers.id_for(obj) != cursor.ref.id
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

  defp cursors!(nil, _parent_ref, _field, _limit), do: []

  defp cursors!({mod, opts}, parent_ref, field, limit) do
    mod.cursors!(parent_ref, field, limit, opts)
  end

  defp cursors!(mod, parent_ref, field, limit) do
    mod.cursors!(parent_ref, field, limit)
  end
end
