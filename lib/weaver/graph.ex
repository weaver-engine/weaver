defmodule Weaver.Graph do
  @moduledoc """
  Stores edges and cursors in the dgraph graph database using the `dlex` client library.
  """

  use GenServer

  alias Weaver.{Cursor, Ref, Store, Marker.ChunkStart, Marker.ChunkEnd}

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
    Enum.each(data, fn
      {%Ref{}, pred, %Ref{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {%Ref{}, pred, %Ref{}, facets}
      when (is_binary(pred) or is_atom(pred)) and is_list(facets) ->
        :ok

      {%Ref{}, pred, obj}
      when (is_binary(pred) or is_atom(pred)) and (is_binary(obj) or is_integer(obj)) ->
        :ok

      other ->
        raise ArgumentError,
              "Invalid data tuple:\n" <> inspect(other, pretty: true, limit: :infinity)
    end)

    Enum.each(meta, fn
      {:add, %Ref{}, pred, %Cursor{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {:del, %Ref{}, pred, %Cursor{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {:add, %Ref{}, pred, %ChunkStart{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {:del, %Ref{}, pred, %ChunkStart{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {:add, %Ref{}, pred, %ChunkEnd{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      {:del, %Ref{}, pred, %ChunkEnd{}} when is_binary(pred) or is_atom(pred) ->
        :ok

      other ->
        raise ArgumentError,
              "Invalid meta tuple:\n" <> inspect(other, pretty: true, limit: :infinity)
    end)

    GenServer.call(__MODULE__, {:store, meta ++ data}, @call_timeout)
  end

  def store!(data, meta) do
    case store(data, meta) do
      :ok -> :ok
      {:ok, result} -> result
      {:error, e} -> raise e
    end
  end

  @impl Store
  def cursors(ref, predicate, opts \\ []) do
    GenServer.call(__MODULE__, {:cursors, ref, predicate, opts}, @call_timeout)
  end

  def cursors!(ref, predicate, opts \\ []) do
    case cursors(ref, predicate, opts) do
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
    {del_tuples, tuples} = Enum.split_with(tuples, &(elem(&1, 0) == :del))

    varnames = varnames_for(tuples)
    query = upsert_query_for(varnames)

    del_varnames = varnames_for(del_tuples)
    del_query = upsert_query_for(del_varnames)

    statements =
      tuples
      |> Enum.flat_map(fn
        {:add, subject, predicate, %Cursor{val: val, gap: gap, ref: %{id: id}}} ->
          sub = property(subject, varnames)
          obj = property(val, varnames)

          facets =
            %{gap: gap, id: id}
            |> Enum.map(fn {k, v} -> "#{k} = #{property(v, varnames)}" end)
            |> Enum.join(", ")

          ["#{sub} <#{predicate}.cursors> #{obj} (#{facets}) ."]

        {:add, subject, predicate, marker = %ChunkStart{cursor: %Cursor{val: val, ref: object}}} ->
          sub = property(subject, varnames)
          obj = property(object, varnames)
          val_var = property(val, varnames)
          marker = property(marker, varnames)

          [
            "#{sub} <weaver.markers> #{marker} .",
            "#{marker} <weaver.markers.predicate> #{inspect(predicate)} .",
            "#{marker} <weaver.markers.intValue> #{val_var} .",
            "#{marker} <weaver.markers.object> #{obj} .",
            ~s|#{marker} <weaver.markers.type> "chunkStart" .|
          ]

        {:add, subject, predicate, marker = %ChunkEnd{cursor: %Cursor{val: val, ref: object}}} ->
          sub = property(subject, varnames)
          obj = property(object, varnames)
          val_var = property(val, varnames)
          marker = property(marker, varnames)

          [
            "#{sub} <weaver.markers> #{marker} .",
            "#{marker} <weaver.markers.predicate> #{inspect(predicate)} .",
            "#{marker} <weaver.markers.intValue> #{val_var} .",
            "#{marker} <weaver.markers.object> #{obj} .",
            ~s|#{marker} <weaver.markers.type> "chunkEnd" .|
          ]

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
        {:del, subject, predicate, %Cursor{val: val, gap: gap, ref: %{id: id}}} ->
          sub = property(subject, del_varnames)
          obj = property(val, del_varnames)

          facets =
            %{gap: gap, id: id}
            |> Enum.map(fn {k, v} -> "#{k} = #{property(v, del_varnames)}" end)
            |> Enum.join(", ")

          ["#{sub} <#{predicate}.cursors> #{obj} (#{facets}) ."]

        {:del, subject, predicate, marker = %ChunkStart{cursor: %Cursor{val: val, ref: object}}} ->
          sub = property(subject, del_varnames)
          obj = property(object, del_varnames)
          val_var = property(val, del_varnames)
          marker = property(marker, del_varnames)

          [
            "#{sub} <weaver.markers> #{marker} .",
            "#{marker} <weaver.markers.predicate> #{inspect(predicate)} .",
            "#{marker} <weaver.markers.intValue> #{val_var} .",
            "#{marker} <weaver.markers.object> #{obj} .",
            ~s|#{marker} <weaver.markers.type> "chunkStart" .|
          ]

        {:del, subject, predicate, marker = %ChunkEnd{cursor: %Cursor{val: val, ref: object}}} ->
          sub = property(subject, del_varnames)
          obj = property(object, del_varnames)
          val_var = property(val, del_varnames)
          marker = property(marker, del_varnames)

          [
            "#{sub} <weaver.markers> #{marker} .",
            "#{marker} <weaver.markers.predicate> #{inspect(predicate)} .",
            "#{marker} <weaver.markers.intValue> #{val_var} .",
            "#{marker} <weaver.markers.object> #{obj} .",
            ~s|#{marker} <weaver.markers.type> "chunkEnd" .|
          ]

        _other ->
          []
      end)

    statement =
      Enum.reduce(varnames, statements, fn
        {%Ref{id: id}, varname}, statements ->
          ["uid(#{varname}) <id> #{inspect(id)} ." | statements]

        {%ChunkStart{}, _varname}, statements ->
          statements

        {%ChunkEnd{}, _varname}, statements ->
          statements
      end)
      |> Enum.join("\n")

    del_statement =
      del_statements
      |> Enum.join("\n")

    result =
      case {statement, del_statement} do
        {"", ""} ->
          :ok

        {"", del_statement} ->
          {:ok, _} = Dlex.delete(Dlex, del_query, del_statement, timeout: @timeout)

        {statement, ""} ->
          {:ok, _} = Dlex.mutate(Dlex, %{query: query}, statement, timeout: @timeout)

        {statement, del_statement} ->
          {:ok, _} = Dlex.delete(Dlex, del_query, del_statement, timeout: @timeout)
          {:ok, _} = Dlex.mutate(Dlex, %{query: query}, statement, timeout: @timeout)
      end

    {:reply, result, state}
  end

  def handle_call({:cursors, ref, predicate, opts}, _from, state) do
    limit_str =
      case Keyword.get(opts, :limit) do
        nil -> ""
        limit when is_integer(limit) -> " (first: #{limit})"
      end

    query = ~s"""
    {
      cursors(func: eq(id, #{inspect(ref.id)})) {
        weaver.markers#{limit_str} @filter(eq(weaver.markers.predicate, "#{predicate}")) (orderdesc: weaver.markers.intValue, orderdesc: weaver.markers.type) {
          weaver.markers.intValue
          weaver.markers.object { id }
          weaver.markers.type
        }
      }
    }
    """

    result =
      case do_query(query) do
        {:ok, %{"cursors" => [%{"weaver.markers" => cursors}]}} ->
          cursors =
            Enum.map(cursors, fn
              %{
                "weaver.markers.intValue" => val,
                "weaver.markers.type" => "chunkStart",
                "weaver.markers.object" => %{"id" => id}
              } ->
                %ChunkStart{cursor: %Cursor{val: val, ref: %Ref{id: id}}}

              %{
                "weaver.markers.intValue" => val,
                "weaver.markers.type" => "chunkEnd",
                "weaver.markers.object" => %{"id" => id}
              } ->
                %ChunkEnd{cursor: %Cursor{val: val, ref: %Ref{id: id}}}
            end)
            |> apply_filters(opts)

          {:ok, cursors}

        {:error, e} ->
          {:error, e}
      end

    {:reply, result, state}
  end

  def handle_call({:count, %Ref{id: id}, relation}, _from, state) do
    result =
      ~s"""
      {
        countRelation(func: eq(id, #{inspect(id)})) {
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

  defp apply_filters(results, []), do: results

  defp apply_filters(results, [{:limit, count} | opts]) do
    results
    |> Enum.take(count)
    |> apply_filters(opts)
  end

  defp apply_filters(results, [{:less_than, val} | opts]) do
    results
    |> Enum.filter(&(&1.cursor.val < val))
    |> apply_filters(opts)
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
        {%ChunkStart{cursor: %Cursor{val: val}}, var} ->
          [
            ~s|q#{var}(func: eq(weaver.markers.intValue, #{val})) @filter(eq(weaver.markers.type, "chunkStart")) { #{
              var
            } as uid }|
          ]

        {%ChunkEnd{cursor: %Cursor{val: val}}, var} ->
          [
            ~s|q#{var}(func: eq(weaver.markers.intValue, #{val})) @filter(eq(weaver.markers.type, "chunkEnd")) { #{
              var
            } as uid }|
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
      {:add, sub = %Ref{}, _pred, obj = %ChunkStart{cursor: %{ref: ref}}} -> [sub, obj, ref]
      {:add, sub = %Ref{}, _pred, obj = %ChunkEnd{cursor: %{ref: ref}}} -> [sub, obj, ref]
      {:add, sub = %Ref{}, _pred, _obj} -> [sub]
      {:del, sub = %Ref{}, _pred, obj = %ChunkStart{cursor: %{ref: ref}}} -> [sub, obj, ref]
      {:del, sub = %Ref{}, _pred, obj = %ChunkEnd{cursor: %{ref: ref}}} -> [sub, obj, ref]
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

  defp property(int, _varnames) when is_integer(int) do
    ~s|"#{int}"^^<xs:int>|
  end

  defp property(other, _varnames), do: inspect(other)

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
end
