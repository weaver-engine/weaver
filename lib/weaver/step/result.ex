defmodule Weaver.Step.Result do
  @moduledoc """
  Functions to initialize and modify the 5-element tuple
  returned as the result of `Weaver.Step.process/1`.
  """

  alias Weaver.{Ref, Step}

  @type t() :: {
          list(tuple()),
          list(tuple()),
          list(tuple()),
          list(Step.t()),
          Step.t() | nil
        }

  def empty() do
    {[], [], [], [], nil}
  end

  def data({data, _, _, _, _}), do: data
  def meta({_, meta, _, _, _}), do: meta
  def errors({_, _, errors, _, _}), do: errors
  def dispatched({_, _, _, dispatched, _}), do: dispatched
  def next({_, _, _, _, next}), do: next

  def add_data({data, meta, errors, dispatched, next}, tuples) when is_list(tuples) do
    {tuples ++ data, meta, errors, dispatched, next}
  end

  def add_data({data, meta, errors, dispatched, next}, tuple) do
    {[tuple | data], meta, errors, dispatched, next}
  end

  def add_relation_data(result, {from = %Ref{}, predicate, [obj | objs]}) do
    result
    |> add_data({from, predicate, Ref.from(obj)})
    |> add_relation_data({from, predicate, objs})
  end

  def add_relation_data(result, {%Ref{}, _predicate, []}), do: result

  def add_meta({data, meta, errors, dispatched, next}, tuples) when is_list(tuples) do
    {data, tuples ++ meta, errors, dispatched, next}
  end

  def add_errors({data, meta, errors, dispatched, next}, new_errors) when is_list(errors) do
    {data, meta, new_errors ++ errors, dispatched, next}
  end

  def dispatch({data, meta, errors, dispatched, next}, tuple) do
    {data, meta, errors, [tuple | dispatched], next}
  end

  def set_next({data, meta, errors, dispatched, _next}, step) do
    {data, meta, errors, dispatched, step}
  end

  def merge(result1, result2) do
    result1
    |> add_data(data(result2))
    |> add_meta(meta(result2))
    |> add_errors(errors(result2))
  end
end
