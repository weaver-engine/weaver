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
      alias Weaver.{Marker, Ref}

      # Default test schema
      alias Weaver.Absinthe.Schema
      alias Weaver.ExTwitter.Mock, as: Twitter
      alias ExTwitter.Model.User, as: TwitterUser
      alias ExTwitter.Model.Tweet

      import Weaver.IntegrationCase
      import Test.Support.Factory

      require Weaver.IntegrationCase
    end
  end

  # Mock helpers

  def twitter_mock_for(user, tweets) do
    fn [id: user_id, tweet_mode: :extended, count: count] ->
      assert user_id == user.id
      Enum.take(tweets, count)
    end
  end

  def twitter_mock_for(user, tweets, max_id: max_id) do
    fn [id: user_id, tweet_mode: :extended, count: count, max_id: ^max_id] ->
      assert user_id == user.id
      {_skipped, tweets} = Enum.split_while(tweets, &(&1.id > max_id))
      Enum.take(tweets, count)
    end
  end

  alias Weaver.Step.Result

  def use_graph(_context) do
    Weaver.Graph.reset!()
  end

  def weave_initial({:ok, step}, mock, fn_name, mock_fun) do
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

  defp weave_step(step) do
    {:ok, result} = Weaver.weave(step)

    Mox.verify!()

    case step.execution.context do
      %{cache: {mod, opts}} when mod != nil ->
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
  defmacro refute_has_data(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)
      data = Result.data(result)

      case unquote(match_expr) do
        subset when is_list(subset) -> Enum.each(subset, &refute(&1 in data))
        tuple when is_tuple(tuple) -> refute tuple in data
      end

      result
    end
  end

  @doc "Compares the result's `data` with the given term."
  def assert_data(result, match) do
    assert Result.data(result) == match

    result
  end

  @doc "Compares the result's `meta` with the given term."
  def assert_meta(result, match) do
    assert Result.meta(result) == match

    result
  end

  @doc "Matches the given expression against the result's `dispatched` paths."
  defmacro assert_dispatched_paths(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)

      paths =
        result
        |> Result.dispatched()
        |> Enum.map(fn
          %{execution: %{acc: %{resolution: paths}}} -> paths
        end)

      assert unquote(match_expr) = paths

      result
    end
  end

  @doc "Matches the given expression against the result's `next` path."
  defmacro assert_next_path(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)

      assert %{
               execution: %{
                 acc: %{
                   resolution: unquote(match_expr)
                 }
               }
             } = Result.next(result)

      result
    end
  end

  @doc "Matches the given expression against the result's `next` Weaver state."
  defmacro assert_next_state(result_expr, match_expr) do
    quote do
      result = unquote(result_expr)

      assert %{
               execution: %{
                 acc: %{
                   Weaver.Absinthe.Middleware.Continue => unquote(match_expr)
                 }
               }
             } = Result.next(result)

      result
    end
  end

  @doc "Expects the result's `next` to be nil."
  def refute_next(result) do
    refute Result.next(result)

    result
  end

  def assert_done(result) do
    assert result == Result.empty()

    result
  end
end
