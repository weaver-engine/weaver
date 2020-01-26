defmodule Weaver.Events do
  @moduledoc """
  Core module handling events.
  """

  alias Weaver.{Cursor, Ref, Resolvers, Tree}

  @doc """
  Takes a job (`Weaver.Tree`) or a list of jobs to be handled.

  Returns a tuple of
  - a list of dispatched jobs that should be handled on the
  next level of the graph (may be an empty list)
  - a job to be handled next on the same level of the graph (may be nil)
  """
  @spec handle(Weaver.Tree.t() | list(Weaver.Tree.t())) ::
          {list(Weaver.Tree.t()), Weaver.Tree.t() | nil}
  def handle(event) do
    do_handle(event)
  end

  def do_handle(events, state \\ nil)

  def do_handle(events, state) when is_list(events) do
    Enum.reduce(events, {[], state}, fn event, {results, state} ->
      {new_results, state} = do_handle(event, state)
      {results ++ new_results, state}
    end)
  end

  def do_handle(event = %Tree{ast: {:document, ops}}, state) do
    continue_with(event, ops, state)
  end

  def do_handle(
        event = %Tree{ast: {:op, _type, _name, _vars, [], fields, _schema_info}},
        state
      ) do
    continue_with(event, fields, state)
  end

  def do_handle(
        event = %Tree{ast: {:frag, :..., {:name, _, _type}, [], fields, _schema_info}},
        state
      ) do
    continue_with(event, fields, state)
  end

  def do_handle(
        event = %Tree{
          ast: {:field, {:name, _, "node"}, [{"id", %{value: id}}], _, fields, _, _schema_info}
        },
        state
      ) do
    id
    |> Resolvers.retrieve_by_id()
    |> store!()
    |> continue_with(event, fields, state)
  end

  def do_handle(
        %Tree{ast: {:field, {:name, _, "id"}, [], [], [], :undefined, _schema_info}},
        state
      ) do
    {[], state}
  end

  def do_handle(
        %Tree{
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
        event = %Tree{
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
        event = %{event | ast: {:retrieve, opts, fields, field}}
        {[event], state}

      obj ->
        obj = store!(obj, [{parent_ref, field}])
        continue_with(obj, event, fields, state)
    end
  end

  def do_handle(
        event = %Tree{
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
      event.refresh && !event.refreshed ->
        gap =
          Weaver.Graph.cursors!(parent_ref, opts, 1)
          |> List.first()

        event = %{event | gap: gap}

        do_handle(event, state)

      event.backfill ->
        Weaver.Graph.cursors!(parent_ref, opts, 3)
        |> Enum.split_while(&(!&1.gap))
        |> case do
          {_refresh_end, [gap_start | rest]} ->
            event = %{event | cursor: gap_start, gap: List.first(rest)}

            do_handle(event, state)

          _else ->
            {[], nil}
        end

      true ->
        {[], nil}
    end
  end

  def do_handle(
        event = %Tree{
          ast: {:retrieve, opts, fields, parent_field},
          data: parent_obj
        },
        _state
      ) do
    parent_ref = parent_obj |> Resolvers.id_for() |> Ref.new()

    {objs, state} =
      case Resolvers.retrieve(parent_obj, opts, event.cursor) do
        {:continue, objs, cursor} ->
          case event.gap do
            %Cursor{ref: %Ref{id: gap_id}} ->
              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
              case Enum.split_while(objs, &(Resolvers.id_for(&1) != gap_id)) do
                {objs, []} ->
                  # gap not closed -> continue with this cursor
                  state = %{event | cursor: cursor, count: event.count + length(objs)}
                  {objs, state}

                {objs, _} ->
                  # gap closed
                  state = %{
                    event
                    | gap: :not_loaded,
                      refreshed: true,
                      count: event.count + length(objs)
                  }

                  {objs, state}
              end

            _else ->
              # no gap -> continue with this cursor
              state = %{event | cursor: cursor, count: event.count + length(objs)}
              {objs, state}
          end

        {:done, objs} ->
          {objs, nil}
      end

    store!(objs, [{parent_ref, parent_field}], event.cursor)

    continue_with(objs, event, fields, state)
  end

  def do_handle(event, _state) do
    raise "Undhandled event:\n\n#{inspect(Map.from_struct(event), pretty: true)}"
  end

  defp continue_with(event, subtree, state) do
    for elem <- subtree do
      %{event | ast: elem}
    end
    |> do_handle(state)
  end

  defp continue_with(objs, event, subtree, state) when is_list(objs) do
    for obj <- objs, elem <- subtree do
      %{event | data: obj, ast: elem}
    end
    |> do_handle(state)
  end

  defp continue_with(obj, event, subtree, state) do
    event
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
