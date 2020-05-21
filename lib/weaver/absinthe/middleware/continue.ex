defmodule Weaver.Absinthe.Middleware.Continue do
  @moduledoc """
  This plugin enables asynchronous execution of a field.
  """

  alias Weaver.{Marker, Ref, Resolvers}

  defstruct [
    :prev_chunk_end,
    next_chunk_start: :not_loaded,
    refresh: true,
    backfill: true,
    refreshed: false,
    count: 0
  ]

  @type t() :: %__MODULE__{
          prev_chunk_end: Marker.t() | nil | :not_loaded,
          next_chunk_start: Marker.t() | nil | :not_loaded,
          refresh: boolean(),
          backfill: boolean(),
          refreshed: boolean(),
          count: non_neg_integer()
        }

  @behaviour Absinthe.Middleware

  # call resolver function only if this is the resolution part for the current step
  def call(%{state: :suspended, acc: %{resolution: path}, path: path} = res, fun) do
    acc = Map.get(res.acc, __MODULE__, %__MODULE__{})

    [%Absinthe.Blueprint.Document.Field{name: field} | _] = path

    parent_ref =
      case res.source do
        nil -> nil
        empty when empty == %{} -> nil
        parent_obj -> Ref.from(parent_obj)
      end

    {prev_chunk_end, next_chunk_start} =
      get_markers(res.context[:cache], acc, parent_ref, field, acc.prev_chunk_end)

    acc = %{acc | prev_chunk_end: prev_chunk_end, next_chunk_start: next_chunk_start}
    step = acc

    resolved = fun.(acc.prev_chunk_end)

    case analyze_resolved(resolved, acc) do
      {:entire_data, []} ->
        meta = meta_delete_all(res.context[:cache], parent_ref, field)

        {[], meta, nil}

      {:entire_data, objs} ->
        meta =
          [{:add, parent_ref, field, Resolvers.start_marker(objs)}] ++
            meta_delete_all(res.context[:cache], parent_ref, field)

        {objs, meta, nil}

      {:last_data, objs} ->
        meta =
          [{:del, parent_ref, field, step.prev_chunk_end}] ++
            meta_delete_all(res.context[:cache], parent_ref, field,
              less_than: step.prev_chunk_end.val
            )

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
    |> case do
      {value, meta, nil} ->
        %{
          res
          | acc: Map.delete(res.acc, __MODULE__) |> Map.put(:meta, meta)
        }
        |> Absinthe.Resolution.put_result({:ok, value})

      {value, meta, next} ->
        new_acc = %{
          acc
          | prev_chunk_end: next.prev_chunk_end,
            next_chunk_start: next.next_chunk_start,
            refreshed: next.refreshed,
            count: next.count
        }

        %{
          res
          | acc: Map.put(res.acc, __MODULE__, new_acc) |> Map.put(:meta, meta),
            middleware: [{__MODULE__, fun} | res.middleware]
            # middleware: [{__MODULE__, {fun, end_marker}} | res.middleware]
        }
        |> Absinthe.Resolution.put_result({:ok, value})
    end
  end

  # ... skip otherwise
  def call(res, _fun) do
    res
  end

  defp first_meta(step = %{prev_chunk_end: %Marker{}}, _resolved, parent_ref, field) do
    {:del, parent_ref, field, step.prev_chunk_end}
  end

  defp first_meta(_step, {:continue, objs, _marker}, parent_ref, field) do
    {:add, parent_ref, field, Resolvers.start_marker(objs)}
  end

  defp meta_delete_all(cache, parent_ref, field, opts \\ []) do
    cache
    |> markers!(parent_ref, field, opts)
    |> Enum.map(&{:del, parent_ref, field, &1})
  end

  defp analyze_resolved({:done, objs}, %{prev_chunk_end: %Marker{}}) do
    {:last_data, objs}
  end

  defp analyze_resolved({:done, objs}, _) do
    {:entire_data, objs}
  end

  # no gap
  defp analyze_resolved({:continue, objs, new_chunk_end}, %{next_chunk_start: nil}) do
    {:continue, objs, new_chunk_end}
  end

  # gap closed?
  defp analyze_resolved({:continue, objs, new_chunk_end}, step = %{}) do
    case Enum.split_while(objs, &before_marker?(&1, step.next_chunk_start)) do
      {objs, []} -> {:continue, objs, new_chunk_end}
      {objs, __} -> {:gap_closed, objs}
    end
  end

  defp before_marker?(obj, marker) do
    Resolvers.marker_val(obj) > marker.val &&
      Resolvers.id_for(obj) != marker.ref.id
  end

  defp get_markers(nil, _, _, _, prev_chunk_end) do
    {prev_chunk_end, nil}
  end

  defp get_markers(_cache, _step, nil, _field, prev_chunk_end) do
    {prev_chunk_end, nil}
  end

  defp get_markers(cache, %{refresh: true, refreshed: false}, parent_ref, field, prev_chunk_end) do
    next_chunk_start =
      markers!(cache, parent_ref, field, limit: 1)
      |> List.first()

    {prev_chunk_end, next_chunk_start}
  end

  defp get_markers(cache, %{backfill: true}, parent_ref, field, prev_chunk_end) do
    markers!(cache, parent_ref, field, limit: 3)
    |> Enum.split_while(&(&1.type != :chunk_end))
    |> case do
      {_refresh_end, [prev_chunk_end | rest]} ->
        {prev_chunk_end, List.first(rest)}

      _else ->
        {prev_chunk_end, nil}
    end
  end

  defp markers!(nil, _parent_ref, _field, _opts), do: []

  defp markers!({mod, cache_opts}, parent_ref, field, opts) do
    mod.markers!(parent_ref, field, Keyword.merge(cache_opts, opts))
  end

  defp markers!(mod, parent_ref, field, opts) do
    IO.inspect({parent_ref, field}, label: "MARKERS")
    mod.markers!(parent_ref, field, opts)
  end
end
