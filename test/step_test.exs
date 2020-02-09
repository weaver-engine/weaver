defmodule Weaver.StepTest do
  use ExUnit.Case, async: true

  import Test.Support.Factory
  import Mox

  alias Weaver.{Cursor, Ref, Step}
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
    step = Weaver.prepare(@query)

    expect(Twitter, :user, fn "elixirdigest" -> user end)
    result = Step.process(step)
    verify!()

    assert {data, [], [step2], nil} = result
    assert {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"} in data

    assert {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count} in data

    assert %{ast: {:retrieve, :favorites, _ast, "favorites"}, cursor: nil, gap: :not_loaded} =
             step2

    assert user == step2.data

    # favorites initial
    expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
      assert user_id == user.id
      Enum.take(favorites, count)
    end)

    result = Step.process(step2)
    verify!()

    assert {data2, meta2, dispatched, step2_} = result

    assert [step3a, step3b] = dispatched

    [tweet2a, tweet2b] = Enum.slice(favorites, 0..1)

    assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2a.id}"}} in data2
    assert {%Ref{id: "Tweet:#{tweet2a.id}"}, "text", tweet2a.full_text} in data2

    assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2b.id}"}} in data2
    assert {%Ref{id: "Tweet:#{tweet2b.id}"}, "text", tweet2b.full_text} in data2

    # assert meta2 == [{%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Cursor{val: 10}}]

    assert %{
             ast: {:retrieve, :favorites, _ast, "favorites"},
             cursor: %Cursor{val: 10},
             gap: nil
           } = step2_

    assert {:retrieve, :retweets, _ast, "retweets"} = step3a.ast
    assert step3a.data == tweet2b
    assert step3b.data == tweet2a

    # favorites pt. 2
    expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count, max_id: 9] ->
      assert user_id == user.id
      Enum.slice(favorites, 2, count)
    end)

    result = Step.process(step2_)
    verify!()

    assert {data2_, meta2_, [step3c, step3d], step2__} = result

    [tweet2c, tweet2d] = Enum.slice(favorites, 2..3)

    assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2c.id}"}} in data2_
    assert {%Ref{id: "Tweet:#{tweet2c.id}"}, "text", tweet2c.full_text} in data2_

    assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2d.id}"}} in data2_
    assert {%Ref{id: "Tweet:#{tweet2d.id}"}, "text", tweet2d.full_text} in data2_

    assert %{ast: {:retrieve, :favorites, _ast, "favorites"}, cursor: %Cursor{val: 8}} = step2__

    assert {:retrieve, :retweets, _ast, "retweets"} = step3c.ast
    assert step3c.data == tweet2d
    assert step3d.data == tweet2c
  end
end
