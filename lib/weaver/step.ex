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

  alias Weaver.{Cursor, Ref, Resolvers, Step}

  @doc """
  Takes a job (`Weaver.Step`) or a list of jobs to be handled.

  Returns a tuple of
  - a list of dispatched jobs that should be handled on the
  next level of the graph (may be an empty list)
  - a job to be handled next on the same level of the graph (may be nil)
  """
  @spec handle(Step.t() | list(Step.t())) :: {list(Step.t()), Step.t() | nil}
  def handle(step) do
    do_handle(step)
  end

  def do_handle(steps, state \\ nil)

  def do_handle(steps, state) when is_list(steps) do
    Enum.reduce(steps, {[], state}, fn step, {results, state} ->
      {new_results, state} = do_handle(step, state)
      {results ++ new_results, state}
    end)
  end

  def do_handle(step = %Step{ast: {:document, ops}}, state) do
    continue_with(step, ops, state)
  end

  def do_handle(
        step = %Step{ast: {:op, _type, _name, _vars, [], fields, _schema_info}},
        state
      ) do
    continue_with(step, fields, state)
  end

  def do_handle(
        step = %Step{ast: {:frag, :..., {:name, _, _type}, [], fields, _schema_info}},
        state
      ) do
    continue_with(step, fields, state)
  end

  def do_handle(
        step = %Step{
          ast: {:field, {:name, _, "node"}, [{"id", %{value: id}}], _, fields, _, _schema_info}
        },
        state
      ) do
    id
    |> Resolvers.retrieve_by_id()
    |> store!()
    |> continue_with(step, fields, state)
  end

  def do_handle(
        %Step{ast: {:field, {:name, _, "id"}, [], [], [], :undefined, _schema_info}},
        state
      ) do
    {[], state}
  end

  def do_handle(
        %Step{
          ast: {:field, {:name, _, field}, [], [], [], :undefined, _schema_info},
          data: parent_obj
        },
        state
      ) do
    value = Resolvers.resolve_leaf(parent_obj, field)

    parent_ref =
      parent_obj
      |> Resolvers.id_for()
      |> Ref.new()

    Weaver.Graph.store!([{parent_ref, field, value}])

    {[], state}
  end

  def do_handle(
        step = %Step{
          ast: {:field, {:name, _, field}, [], [], fields, :undefined, _schema_info},
          data: parent_obj
        },
        state
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
        {[step], state}

      obj ->
        obj = store!(obj, [{parent_ref, field}])
        continue_with(obj, step, fields, state)
    end
  end

  def do_handle(
        step = %Step{
          ast: {:retrieve, opts, _fields, _parent_field},
          data: parent_obj,
          gap: :not_loaded
        },
        state
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

        do_handle(step, state)

      step.backfill ->
        Weaver.Graph.cursors!(parent_ref, opts, 3)
        |> Enum.split_while(&(!&1.gap))
        |> case do
          {_refresh_end, [gap_start | rest]} ->
            step = %{step | cursor: gap_start, gap: List.first(rest)}

            do_handle(step, state)

          _else ->
            {[], nil}
        end

      true ->
        {[], nil}
    end
  end

  def do_handle(
        step = %Step{
          ast: {:retrieve, opts, fields, parent_field},
          data: parent_obj
        },
        _state
      ) do
    parent_ref = parent_obj |> Resolvers.id_for() |> Ref.new()

    {objs, state} =
      case Resolvers.retrieve(parent_obj, opts, step.cursor) do
        {:continue, objs, cursor} ->
          case step.gap do
            %Cursor{ref: %Ref{id: gap_id}} ->
              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
              case Enum.split_while(objs, &(Resolvers.id_for(&1) != gap_id)) do
                {objs, []} ->
                  # gap not closed -> continue with this cursor
                  state = %{step | cursor: cursor, count: step.count + length(objs)}
                  {objs, state}

                {objs, _} ->
                  # gap closed
                  state = %{
                    step
                    | gap: :not_loaded,
                      refreshed: true,
                      count: step.count + length(objs)
                  }

                  {objs, state}
              end

            _else ->
              # no gap -> continue with this cursor
              state = %{step | cursor: cursor, count: step.count + length(objs)}
              {objs, state}
          end

        {:done, objs} ->
          {objs, nil}
      end

    store!(objs, [{parent_ref, parent_field}], step.cursor)

    continue_with(objs, step, fields, state)
  end

  def do_handle(step, _state) do
    raise "Undhandled step:\n\n#{inspect(Map.from_struct(step), pretty: true)}"
  end

  defp continue_with(step, subtree, state) do
    for elem <- subtree do
      %{step | ast: elem}
    end
    |> do_handle(state)
  end

  defp continue_with(objs, step, subtree, state) when is_list(objs) do
    for obj <- objs, elem <- subtree do
      %{step | data: obj, ast: elem}
    end
    |> do_handle(state)
  end

  defp continue_with(obj, step, subtree, state) do
    step
    |> Map.put(:data, obj)
    |> continue_with(subtree, state)
  end

  defp store!(objs, relations \\ [], old_cursor \\ nil, gap_cursor \\ nil)

  defp store!([], _relations, _, _), do: []

  defp store!(objs, relations, old_cursor, gap_cursor) when is_list(objs) do
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

    Weaver.Graph.store!(tuples)

    objs
  end

  defp store!(obj, relations, old_cursor, gap_cursor) do
    [obj] = store!([obj], relations, old_cursor, gap_cursor)
    obj
  end
end
