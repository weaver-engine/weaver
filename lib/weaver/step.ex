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
                :source_graph,
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
          source_graph: module() | nil,
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
    Functions to modify the 4-element tuple returned as the result of `Weaver.Step.process/1`.
    """

    alias Weaver.{Cursor, Ref, Resolvers, Step}

    def add_data({data, meta, dispatched, next}, tuples) when is_list(tuples) do
      {tuples ++ data, meta, dispatched, next}
    end

    def add_data({data, meta, dispatched, next}, tuple) do
      {[tuple | data], meta, dispatched, next}
    end

    def add_data(result, objs, relations \\ [], old_cursor \\ nil, gap_cursor \\ nil)
        when is_list(objs) do
      [{last_sub, last_pred, last_obj} | tuples] =
        Enum.flat_map(objs, fn obj ->
          id = Resolvers.id_for(obj)
          ref = Ref.new(id)

          # cursor_tuple =
          #   case Resolvers.cursor(obj) do
          #     cursor when is_integer(cursor) -> {ref, "weaver.cursor.int", cursor}
          #     cursor when is_binary(cursor) -> {ref, "weaver.cursor.str", cursor}
          #   end

          relation_tuples =
            Enum.map(relations, fn {from = %Ref{}, relation} ->
              {from, relation, ref}
            end)

          [{ref, :id, id} | relation_tuples]
        end)
        |> Enum.reverse()

      tuples =
        if gap_cursor do
          cursor = Resolvers.cursor(last_obj)
          [{last_sub, last_pred, last_obj, cursor: cursor.val, gap: true} | tuples]
        else
          [{last_sub, last_pred, last_obj} | tuples]
        end
        |> Enum.reverse()

      tuples =
        if old_cursor do
          relation_tuples =
            Enum.map(relations, fn {from = %Ref{}, relation} ->
              {from, relation, old_cursor.ref, []}
            end)

          relation_tuples ++ tuples
        else
          tuples
        end

      Result.add_data(result, tuples)
    end

    def dispatch({data, meta, dispatched, next}, tuple) do
      {data, meta, [tuple | dispatched], next}
    end

    def next({data, meta, dispatched, _next}, step) do
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
    do_process(step)
  end

  def do_process(steps, result \\ {[], [], [], nil})

  def do_process(steps, result) when is_list(steps) do
    Enum.reduce(steps, result, &do_process/2)
  end

  def do_process(step = %Step{ast: {:document, ops}}, result) do
    continue_with(step, ops, result)
  end

  def do_process(
        step = %Step{ast: {:op, _type, _name, _vars, [], fields, _schema_info}},
        result
      ) do
    continue_with(step, fields, result)
  end

  def do_process(
        step = %Step{ast: {:frag, :..., {:name, _, _type}, [], fields, _schema_info}},
        result
      ) do
    continue_with(step, fields, result)
  end

  def do_process(
        step = %Step{
          ast: {:field, {:name, _, "node"}, [{"id", %{value: id}}], _, fields, _, _schema_info}
        },
        result
      ) do
    obj = Resolvers.retrieve_by_id(id)
    ref = Ref.new(id)

    new_step = %{step | data: obj}
    new_result = Result.add_data(result, {ref, :id, id})
    continue_with(new_step, fields, new_result)
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

    parent_ref =
      parent_obj
      |> Resolvers.id_for()
      |> Ref.new()

    Result.add_data(result, {parent_ref, field, value})
  end

  def do_process(
        step = %Step{
          ast: {:field, {:name, _, field}, [], [], fields, :undefined, _schema_info},
          data: parent_obj
        },
        result
      ) do
    parent_ref =
      parent_obj
      |> Resolvers.id_for()
      |> Ref.new()

    # with total_count = Resolvers.total_count(parent_obj, field),
    #      count = Weaver.Graph.count!(Resolvers.id_for(parent_obj), field),
    #      count == total_count do
    #       Weaver.Graph.stream(Resolvers.id_for(parent_obj), field)
    case Resolvers.resolve_node(parent_obj, field) do
      {:retrieve, ^parent_obj, opts} ->
        step = %{step | ast: {:retrieve, opts, fields, field}}
        Result.dispatch(result, step)

      obj ->
        obj_ref =
          obj
          |> Resolvers.id_for()
          |> Ref.new()

        new_result = Result.add_data(result, {parent_ref, field, obj_ref})
        continue_with(obj, step, fields, new_result)
    end
  end

  def do_process(
        step = %Step{
          ast: {:retrieve, opts, _fields, _parent_field},
          data: parent_obj,
          gap: :not_loaded
        },
        result
      ) do
    parent_ref =
      parent_obj
      |> Resolvers.id_for()
      |> Ref.new()

    cond do
      step.refresh && !step.refreshed ->
        gap =
          Weaver.Graph.cursors!(parent_ref, opts, 1)
          |> List.first()

        step = %{step | gap: gap}

        do_process(step, result)

      step.backfill ->
        Weaver.Graph.cursors!(parent_ref, opts, 3)
        |> Enum.split_while(&(!&1.gap))
        |> case do
          {_refresh_end, [gap_start | rest]} ->
            step = %{step | cursor: gap_start, gap: List.first(rest)}

            do_process(step, result)

          _else ->
            {[], nil}
        end

      true ->
        {[], nil}
    end
  end

  def do_process(
        step = %Step{
          ast: {:retrieve, opts, fields, parent_field},
          data: parent_obj
        },
        result
      ) do
    parent_ref = parent_obj |> Resolvers.id_for() |> Ref.new()

    {objs, next} =
      case Resolvers.retrieve(parent_obj, opts, step.cursor) do
        {:continue, objs, cursor} ->
          case step.gap do
            %Cursor{ref: %Ref{id: gap_id}} ->
              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
              case Enum.split_while(objs, &(Resolvers.id_for(&1) != gap_id)) do
                {objs, []} ->
                  # gap not closed -> continue with this cursor
                  next = %{step | cursor: cursor, count: step.count + length(objs)}
                  {objs, next}

                {objs, _} ->
                  # gap closed
                  next = %{
                    step
                    | gap: :not_loaded,
                      refreshed: true,
                      count: step.count + length(objs)
                  }

                  {objs, next}
              end

            _else ->
              # no gap -> continue with this cursor
              next = %{step | cursor: cursor, count: step.count + length(objs)}
              {objs, next}
          end

        {:done, objs} ->
          {objs, nil}
      end

    new_result =
      result
      |> Result.add_data(objs, [{parent_ref, parent_field}], step.cursor)
      |> Result.next(next)

    continue_with(objs, step, fields, new_result)
  end

  def do_process(step, _next) do
    raise "Undhandled step:\n\n#{inspect(Map.from_struct(step), pretty: true)}"
  end

  defp continue_with(step, subtree, result) do
    for elem <- subtree do
      %{step | ast: elem}
    end
    |> do_process(result)
  end

  defp continue_with(objs, step, subtree, result) when is_list(objs) do
    for obj <- objs, elem <- subtree do
      %{step | data: obj, ast: elem}
    end
    |> do_process(result)
  end

  defp continue_with(obj, step, subtree, result) do
    step
    |> Map.put(:data, obj)
    |> continue_with(subtree, result)
  end
end
