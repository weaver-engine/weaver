defmodule Weaver.Graph do
  @moduledoc """
  Stores edges and markers in the dgraph graph database using the `dlex` client library.
  """

  use GenServer

  alias Weaver.{Marker, Ref, Store}

  @behaviour Store

  @timeout :timer.seconds(60)
  @call_timeout :timer.seconds(75)
  @indexes [
    "id: string @index(hash,trigram) @upsert .",
    "weaver.markers.intValue: int @index(int) .",
    "weaver.markers.predicate: string .",
    "weaver.markers.type: string .",
    "weaver.markers.object: uid ."
  ]

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl Store
  def store(data, meta) do
    with :ok <- validate_data(data),
         :ok <- validate_meta(meta) do
      GenServer.call(__MODULE__, {:store, meta ++ data}, @call_timeout)
    end
  end

  defp validate_data([]) do
    :ok
  end

  defp validate_data([{%Ref{}, pred, %Ref{}} | rest]) when is_binary(pred) or is_atom(pred) do
    validate_data(rest)
  end

  defp validate_data([{%Ref{}, pred, %Ref{}, facets} | rest])
       when (is_binary(pred) or is_atom(pred)) and is_list(facets) do
    validate_data(rest)
  end

  defp validate_data([{%Ref{}, pred, obj} | rest])
       when (is_binary(pred) or is_atom(pred)) and (is_binary(obj) or is_integer(obj)) do
    validate_data(rest)
  end

  defp validate_data([other | _rest]) do
    {:error,
     %RuntimeError{
       message: "Invalid data tuple:\n" <> inspect(other, pretty: true, limit: :infinity)
     }}
  end

  defp validate_meta([]) do
    :ok
  end

  defp validate_meta([{op, %Ref{}, pred, %Marker{ref: %Ref{id: id}, val: val}} | rest])
       when op in [:add, :del] and is_binary(pred) and is_binary(id) and is_integer(val) do
    validate_meta(rest)
  end

  defp validate_meta([other | _rest]) do
    {:error,
     %RuntimeError{
       message: "Invalid meta tuple:\n" <> inspect(other, pretty: true, limit: :infinity)
     }}
  end

  def store!(data, meta) do
    case store(data, meta) do
      :ok -> :ok
      {:ok, result} -> result
      {:error, e} -> raise e
    end
  end

  @impl Store
  def markers(ref, predicate, opts \\ []) do
    GenServer.call(__MODULE__, {:markers, ref, predicate, opts}, @call_timeout)
  end

  def markers!(ref, predicate, opts \\ []) do
    case markers(ref, predicate, opts) do
      {:ok, result} -> result
      {:error, e} -> raise e
    end
  end

  @impl Store
  def count(ref, relation) do
    GenServer.call(__MODULE__, {:count, ref, relation}, @call_timeout)
  end

  def count!(id, relation) do
    case count(id, relation) do
      {:ok, result} -> result
      {:error, e} -> raise e
    end
  end

  @impl Store
  def reset() do
    GenServer.call(__MODULE__, :reset, @call_timeout)
  end

  def reset!() do
    case reset() do
      :ok -> :ok
      {:error, e} -> raise e
    end
  end

  def query(query) do
    GenServer.call(__MODULE__, {:query, query}, @call_timeout)
  end

  @impl GenServer
  def init(_args) do
    {:ok, %{}}
  end

  @impl GenServer
  def handle_call({:store, tuples}, _from, state) do
    {del_tuples, add_tuples} = Enum.split_with(tuples, &(elem(&1, 0) == :del))

    varnames = varnames_for(tuples)
    query = upsert_query_for(varnames)

    add_statements =
      add_tuples
      |> Enum.flat_map(fn
        {:add, subject, predicate, marker = %Marker{}} ->
          sub_var = property(subject, varnames)
          ref_var = property(marker.ref, varnames)
          val_var = property(marker.val, varnames)
          marker_var = property(marker, varnames)
          type_str = marker_type_str(marker.type)

          [
            "#{sub_var} <weaver.markers> #{marker_var} .",
            "#{marker_var} <weaver.markers.predicate> #{property(predicate)} .",
            "#{marker_var} <weaver.markers.intValue> #{val_var} .",
            "#{marker_var} <weaver.markers.object> #{ref_var} .",
            "#{marker_var} <weaver.markers.type> #{property(type_str)} ."
          ]
          |> maybe_add_nquad(marker_var, "weaver.markers.cursor", marker.cursor)

        {subject, predicate, object} ->
          sub = property(subject, varnames)
          obj = property(object, varnames)
          ["#{sub} <#{predicate}> #{obj} ."]

        {subject, predicate, object, []} ->
          sub = property(subject, varnames)
          obj = property(object, varnames)
          ["#{sub} <#{predicate}> #{obj} ."]

        {subject, predicate, object, facets} ->
          sub = property(subject, varnames)
          obj = property(object, varnames)

          facets =
            facets
            |> Enum.map(fn {k, v} -> "#{k} = #{property(v, varnames)}" end)
            |> Enum.join(", ")

          ["#{sub} <#{predicate}> #{obj} (#{facets}) ."]
      end)

    del_statements =
      del_tuples
      |> Enum.flat_map(fn
        {:del, subject, predicate, marker = %Marker{}} ->
          sub_var = property(subject, varnames)
          ref_var = property(marker.ref, varnames)
          val_var = property(marker.val, varnames)
          marker_var = property(marker, varnames)
          type_str = marker_type_str(marker.type)

          [
            "#{sub_var} <weaver.markers> #{marker_var} .",
            "#{marker_var} <weaver.markers.predicate> #{property(predicate)} .",
            "#{marker_var} <weaver.markers.intValue> #{val_var} .",
            "#{marker_var} <weaver.markers.object> #{ref_var} .",
            "#{marker_var} <weaver.markers.type> #{property(type_str)} ."
          ]
          |> maybe_add_nquad(marker_var, "weaver.markers.cursor", marker.cursor)

        _other ->
          []
      end)

    id_statements =
      Enum.flat_map(varnames, fn
        {%Ref{id: id}, varname} ->
          ["uid(#{varname}) <id> #{property(id)} ."]

        {%Marker{}, _varname} ->
          []
      end)

    result =
      [delete: del_statements, set: add_statements ++ id_statements]
      |> Enum.flat_map(fn
        {_op, []} -> []
        {op, list} -> [%{op => Enum.join(list, "\n")}]
      end)
      |> case do
        [] -> :ok
        mutations -> Dlex.mutate(Dlex, %{query: query}, mutations, timeout: @timeout)
      end

    {:reply, result, state}
  end

  def handle_call({:markers, ref, predicate, opts}, _from, state) do
    limit_str =
      case Keyword.get(opts, :limit) do
        nil -> ""
        limit when is_integer(limit) -> ", first: #{limit}"
      end

    filters =
      Enum.flat_map(opts, fn
        {:less_than, val} -> ["lt(weaver.markers.intValue, #{val})"]
        _else -> []
      end)

    filters = [~s|eq(weaver.markers.predicate, "#{predicate}")| | filters]

    filters_str = Enum.join(filters, " AND ")

    query = ~s"""
    {
      markers(func: eq(id, #{property(ref.id)})) {
        weaver.markers @filter(#{filters_str}) (orderdesc: weaver.markers.intValue, orderdesc: weaver.markers.type#{
      limit_str
    }) {
          weaver.markers.intValue
          weaver.markers.cursor
          weaver.markers.object { id }
          weaver.markers.type
        }
      }
    }
    """

    result =
      case do_query(query) do
        {:ok, %{"markers" => []}} ->
          {:ok, []}

        {:ok, %{"markers" => [%{"weaver.markers" => markers}]}} ->
          markers =
            Enum.map(markers, fn
              %{
                "weaver.markers.intValue" => val,
                "weaver.markers.type" => type_str,
                "weaver.markers.object" => %{"id" => id}
              } = payload ->
                %Marker{
                  type: to_marker_type(type_str),
                  val: val,
                  ref: %Ref{id: id},
                  cursor: payload["weaver.markers.cursor"]
                }
            end)

          {:ok, markers}

        {:error, e} ->
          {:error, e}
      end

    {:reply, result, state}
  end

  def handle_call({:count, %Ref{id: id}, relation}, _from, state) do
    result =
      ~s"""
      {
        countRelation(func: eq(id, #{property(id)})) {
          c : count(#{relation})
        }
      }
      """
      |> do_query()
      |> case do
        {:ok, %{"countRelation" => [%{"c" => count}]}} -> {:ok, count}
        {:ok, %{"countRelation" => []}} -> {:ok, nil}
        {:error, e} -> {:error, e}
      end

    {:reply, result, state}
  end

  def handle_call(:reset, _from, _uids) do
    result =
      with {:ok, _result} <- Dlex.alter(Dlex, %{drop_all: true}, timeout: @timeout),
           {:ok, _result} <- Dlex.alter(Dlex, Enum.join(@indexes, "\n"), timeout: @timeout) do
        :ok
      end

    {:reply, result, %{}}
  end

  def handle_call({:query, query}, _from, _uids) do
    result = do_query(query)

    {:reply, result, %{}}
  end

  defp do_query(query) do
    Dlex.query(Dlex, query, %{}, timeout: @timeout)
  end

  @doc ~S"""
  Generates a upsert query for dgraph.

  ## Examples

      iex> %{
      ...>   Weaver.Ref.new("id1") => "a",
      ...>   Weaver.Ref.new("id2") => "b",
      ...>   Weaver.Ref.new("id3") => "c"
      ...> }
      ...> |> Weaver.Graph.upsert_query_for()
      "{ a as var(func: eq(id, \"id1\"))
      b as var(func: eq(id, \"id2\"))
      c as var(func: eq(id, \"id3\")) }"
  """
  def upsert_query_for(varnames) do
    query_body =
      varnames
      |> Enum.flat_map(fn
        {%Marker{type: type, val: val}, var} ->
          type_str = marker_type_str(type)

          [
            ~s|q#{var}(func: eq(weaver.markers.intValue, #{val})) @filter(eq(weaver.markers.type, "#{
              type_str
            }")) { #{var} as uid }|
          ]

        {%Ref{id: id}, var} ->
          [~s|#{var} as var(func: eq(id, "#{id}"))|]
      end)
      |> Enum.join("\n")

    "{ #{query_body} }"
  end

  @doc """
  Assigns a unique variable name to be used in dgraph upserts according to a sequence.

  ## Examples

      iex> [
      ...>   {Weaver.Ref.new("id1"), :follows, Weaver.Ref.new("id2")},
      ...>   {Weaver.Ref.new("id1"), :name, "Kiara"},
      ...>   {Weaver.Ref.new("id2"), :name, "Greg"},
      ...>   {Weaver.Ref.new("id3"), :name, "Sia"},
      ...>   {Weaver.Ref.new("id2"), :follows, Weaver.Ref.new("id4"), friend: true},
      ...>   {Weaver.Ref.new("id5"), :follows, Weaver.Ref.new("id6"), friend: true}
      ...> ]
      ...> |> Weaver.Graph.varnames_for()
      %{
        Weaver.Ref.new("id1") => "a",
        Weaver.Ref.new("id2") => "b",
        Weaver.Ref.new("id3") => "c",
        Weaver.Ref.new("id4") => "d",
        Weaver.Ref.new("id5") => "aa",
        Weaver.Ref.new("id6") => "ab"
      }
  """
  def varnames_for(tuples) do
    tuples
    |> Enum.flat_map(fn
      {sub = %Ref{}, _pred, obj = %Ref{}} -> [sub, obj]
      {sub = %Ref{}, _pred, obj = %Ref{}, _facets} -> [sub, obj]
      {sub = %Ref{}, _pred, _obj} -> [sub]
      {:add, sub = %Ref{}, _pred, marker = %Marker{}} -> [sub, marker, marker.ref]
      {:add, sub = %Ref{}, _pred, _obj} -> [sub]
      {:del, sub = %Ref{}, _pred, marker = %Marker{}} -> [sub, marker, marker.ref]
      {:del, sub = %Ref{}, _pred, _obj} -> [sub]
      {sub = %Ref{}, _pred, _obj, _facets} -> [sub]
    end)
    |> Enum.reduce(%{}, fn id, map ->
      Map.put_new(map, id, varname(map_size(map)))
    end)
  end

  defp property(ref = %_struct{}, varnames) do
    with {:ok, var} <- Map.fetch(varnames, ref) do
      "uid(#{var})"
    end
  end

  defp property(other, _varnames) do
    property(other)
  end

  defp property(int) when is_integer(int) do
    ~s|"#{int}"^^<xs:int>|
  end

  defp property(other), do: inspect(other)

  defp maybe_add_nquad(list, _sub, _pred, nil) do
    list
  end

  defp maybe_add_nquad(list, sub, pred, obj) do
    ["#{sub} <#{pred}> #{property(obj)} ." | list]
  end

  @doc """
  Generates a variable name from a number.

  ## Examples

      iex> Weaver.Graph.varname(4)
      "aa"

      iex> Weaver.Graph.varname(5)
      "ab"

      iex> Weaver.Graph.varname(6)
      "ac"

      iex> Weaver.Graph.varname(7)
      "ad"

      iex> Weaver.Graph.varname(8)
      "ba"

      iex> Weaver.Graph.varname(19)
      "dd"

      iex> Weaver.Graph.varname(20)
      "aaa"

      iex> Weaver.Graph.varname(83)
      "ddd"

      iex> Weaver.Graph.varname(84)
      "aaaa"
  """
  def varname(0), do: "a"
  def varname(1), do: "b"
  def varname(2), do: "c"
  def varname(3), do: "d"

  def varname(n) do
    varname(div(n, 4) - 1) <> varname(rem(n, 4))
  end

  defp marker_type_str(:chunk_start), do: "chunkStart"
  defp marker_type_str(:chunk_end), do: "chunkEnd"

  defp to_marker_type("chunkStart"), do: :chunk_start
  defp to_marker_type("chunkEnd"), do: :chunk_end
end
