defmodule Weaver.StepTest do
  use ExUnit.Case, async: true

  import Test.Support.Factory
  import Mox

  alias Weaver.{Cursor, Step}
  alias Weaver.ExTwitter.Mock, as: Twitter

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        id
        screenName
        favoritesCount
        favorites {
          text
          publishedAt
          user {
            screenName
          }
          retweetsCount
          retweets {
            text
            publishedAt
            user {
              screenName
            }
          }
        }
      }
    }
  }
  """

  setup do
    {:ok, _pid} = Weaver.Graph.start_link(nil)

    {:ok, _pid} =
      Application.get_env(:weaver, :dgraph, [])
      |> Keyword.merge(name: Dlex)
      |> Dlex.start_link()

    Weaver.Graph.reset!()
    :ok
  end

  setup do
    user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
    favorites = build(ExTwitter.Model.Tweet, 10, fn i -> [id: 11 - i] end)

    {:ok, user: user, favorites: favorites}
  end

  test "", %{user: user, favorites: favorites} do
    {ast, fun_env} = Weaver.parse_query(@query)
    step = %Step{ast: ast, fun_env: fun_env}

    expect(Twitter, :user, fn "elixirdigest" -> user end)
    result = Step.handle(step)
    verify!()

    assert {[step2], nil} = result

    assert %{ast: {:retrieve, :favorites, _ast, "favorites"}, cursor: nil, gap: :not_loaded} =
             step2

    assert user == step2.data

    # favorites initial
    expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
      assert user_id == user.id
      Enum.take(favorites, count)
    end)

    result = Step.handle(step2)
    verify!()

    assert {[step3a, step3b], step2_} = result

    assert %{ast: {:retrieve, :favorites, _ast, "favorites"}, cursor: %Cursor{val: 10}, gap: nil} =
             step2_

    assert {:retrieve, :retweets, _ast, "retweets"} = step3a.ast
    assert Enum.at(favorites, 0) == step3a.data
    assert Enum.at(favorites, 1) == step3b.data

    # favorites pt. 2
    expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count, max_id: 9] ->
      assert user_id == user.id
      Enum.slice(favorites, 2, count)
    end)

    result = Step.handle(step2_)
    verify!()

    assert {[step3c, step3d], step2__} = result

    assert %{ast: {:retrieve, :favorites, _ast, "favorites"}, cursor: %Cursor{val: 8}} = step2__

    assert {:retrieve, :retweets, _ast, "retweets"} = step3c.ast
    assert Enum.at(favorites, 2) == step3c.data
    assert Enum.at(favorites, 3) == step3d.data
  end
end
