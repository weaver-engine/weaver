defmodule Weaver.IntegrationCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a store.

  Such tests rely on `Weaver.Store` and also
  import other functionality to make it easier
  to build common data structures and query the schema.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test resets
  the store in use at the beginning of the test.
  """

  use ExUnit.CaseTemplate

  using do
    quote location: :keep do
      alias Weaver.{Cursor, Ref}

      import Weaver.IntegrationCase
      import Test.Support.Factory

      require Weaver.IntegrationCase
    end
  end

  alias Weaver.Step.Result

  def use_graph(_context) do
    Weaver.Graph.reset!()
  end

  def weave_initial(step = %Weaver.Step{}, mock, fn_name, mock_fun) do
    Mox.expect(mock, fn_name, mock_fun)

    weave_step(step)
  end

  def weave_dispatched(result, index \\ 0, mock, fn_name, mock_fun) do
    Mox.expect(mock, fn_name, mock_fun)

    result
    |> Result.dispatched()
    |> Enum.at(index)
    |> weave_step()
  end

  def weave_next(result, mock, fn_name, mock_fun) do
    Mox.expect(mock, fn_name, mock_fun)

    weave_next(result)
  end

  def weave_next(result) do
    result
    |> Result.next()
    |> weave_step()
  end

  defp weave_step(step = %Weaver.Step{}) do
    result = Weaver.weave(step)

    Mox.verify!()

    case step do
      %{cache: {mod, opts}} ->
        assert mod.store!(Result.data(result), Result.meta(result), opts)

      %{cache: mod} when mod != nil ->
        assert mod.store!(Result.data(result), Result.meta(result))

      _else ->
        nil
    end

    result
  end

  @doc "Matches the given expression against the result's `data`."
  defmacro assert_has_data(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      data = Result.data(result)

      case unquote(match_expr) do
        subset when is_list(subset) -> Enum.each(subset, &assert(&1 in data))
        tuple when is_tuple(tuple) -> assert tuple in data
      end

      result
    end
  end

  @doc "Matches the given expression against the result's `data`."
  defmacro assert_data(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      assert unquote(match_expr) = Result.data(result)

      result
    end
  end

  @doc "Matches the given expression against the result's `meta`."
  defmacro assert_meta(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      assert unquote(match_expr) = Result.meta(result)

      result
    end
  end

  @doc "Matches the given expression against the result's `dispatched`."
  defmacro assert_dispatched(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      assert unquote(match_expr) = Result.dispatched(result)

      result
    end
  end

  @doc "Matches the given expression against the result's `next`."
  defmacro assert_next(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      assert unquote(match_expr) = Result.next(result)

      result
    end
  end

  @doc "Matches the given expression against the result's `next`."
  defmacro refute_next(result_expr) do
    quote do
      result = unquote(result_expr)
      refute Result.next(result)

      result
    end
  end

  defmacro assert_done(result_expr) do
    quote do
      result = unquote(result_expr)
      assert result == Result.new()

      result
    end
  end
end
