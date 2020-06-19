defmodule Weaver.Step do
  @moduledoc """
  Core processing logic for each chunk of streamed data.
  """
  alias Weaver.Marker

  def process_resolved(resolved, step, cache, parent_ref, field, order, id_for) do
    case analyze_resolved(resolved, step, order, id_for) do
      {:entire_data, []} ->
        meta = meta_delete_all(cache, parent_ref, field)

        {[], meta, nil}

      {:entire_data, objs} ->
        meta =
          [{:add, parent_ref, field, start_marker(objs, order, id_for)}] ++
            meta_delete_all(cache, parent_ref, field)

        {objs, meta, nil}

      {:last_data, objs} ->
        meta =
          [{:del, parent_ref, field, step.prev_chunk_end}] ++
            meta_delete_all(cache, parent_ref, field, less_than: step.prev_chunk_end.val)

        {objs, meta, nil}

      # no gap or gap not closed -> continue with this marker
      {:continue, objs, cursor} ->
        new_chunk_end = end_marker(objs, order, id_for, cursor)

        meta = [
          first_meta(step, resolved, parent_ref, field, order, id_for),
          {:add, parent_ref, field, new_chunk_end}
        ]

        next = %{step | prev_chunk_end: new_chunk_end, count: step.count + length(objs)}

        {objs, meta, next}

      # gap closed -> look up the next chunk start in next iteration
      {:gap_closed, objs} ->
        meta = [
          first_meta(step, resolved, parent_ref, field, order, id_for),
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
  end

  defp first_meta(
         step = %{prev_chunk_end: %Marker{}},
         _resolved,
         parent_ref,
         field,
         _order,
         _id_for
       ) do
    {:del, parent_ref, field, step.prev_chunk_end}
  end

  defp first_meta(_step, {:continue, objs, _marker}, parent_ref, field, order, id_for) do
    {:add, parent_ref, field, start_marker(objs, order, id_for)}
  end

  defp meta_delete_all(cache, parent_ref, field, opts \\ []) do
    cache
    |> markers!(parent_ref, field, opts)
    |> Enum.map(&{:del, parent_ref, field, &1})
  end

  defp analyze_resolved({:done, objs}, %{prev_chunk_end: %Marker{}}, _order, _id_for) do
    {:last_data, objs}
  end

  defp analyze_resolved({:done, objs}, _, _order, _id_for) do
    {:entire_data, objs}
  end

  # no gap
  defp analyze_resolved({:continue, objs, cursor}, %{next_chunk_start: nil}, _order, _id_for) do
    {:continue, objs, cursor}
  end

  # gap closed?
  defp analyze_resolved({:continue, objs, cursor}, step = %{}, order, id_for) do
    case Enum.split_while(objs, &before_marker?(&1, step.next_chunk_start, order, id_for)) do
      {objs, []} -> {:continue, objs, cursor}
      {objs, __} -> {:gap_closed, objs}
    end
  end

  defp before_marker?(obj, marker, %{ordered_by: order_field}, id_for) do
    Map.get(obj, order_field) > marker.val &&
      id_for.(obj) != marker.ref.id
  end

  def start_marker(objs, %{ordered_by: order_field}, id_for) do
    obj = List.first(objs)
    id = id_for.(obj)
    val = Map.get(obj, order_field)
    Marker.chunk_start(id, val)
  end

  def end_marker(objs, %{ordered_by: order_field}, id_for, cursor) do
    obj = List.last(objs)
    id = id_for.(obj)
    val = Map.get(obj, order_field)
    Marker.chunk_end(id, val, cursor)
  end

  def load_markers(step = %{next_chunk_start: val}, _opts, _cache, _parent_ref, _field)
      when val != :not_loaded do
    step
  end

  def load_markers(step, _opts, nil, _parent_ref, _field) do
    %{step | next_chunk_start: nil}
  end

  def load_markers(step, _opts, _cache, nil, _field) do
    %{step | next_chunk_start: nil}
  end

  def load_markers(step = %{refreshed: false}, %{refresh: true}, cache, parent_ref, field) do
    next_chunk_start =
      markers!(cache, parent_ref, field, limit: 1)
      |> List.first()

    %{step | next_chunk_start: next_chunk_start}
  end

  def load_markers(step, %{backfill: true}, cache, parent_ref, field) do
    markers!(cache, parent_ref, field, limit: 3)
    |> Enum.split_while(&(&1.type != :chunk_end))
    |> case do
      {_refresh_end, [prev_chunk_end | rest]} ->
        %{step | prev_chunk_end: prev_chunk_end, next_chunk_start: List.first(rest)}

      _else ->
        %{step | next_chunk_start: nil}
    end
  end

  defp markers!(nil, _parent_ref, _field, _opts), do: []

  defp markers!({mod, cache_opts}, parent_ref, field, opts) do
    mod.markers!(parent_ref, field, Keyword.merge(cache_opts, opts))
  end

  defp markers!(mod, parent_ref, field, opts) do
    mod.markers!(parent_ref, field, opts)
  end
end
