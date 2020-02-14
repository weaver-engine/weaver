defmodule WeaverTest do
  use ExUnit.Case, async: true

  import Test.Support.Factory
  import Mox

  alias Weaver.{Cursor, Ref}
  alias Weaver.ExTwitter.Mock, as: Twitter

  def use_graph(_context) do
    {:ok, _pid} = Weaver.Graph.start_link(nil)

    {:ok, _pid} =
      Application.get_env(:weaver, :dgraph, [])
      |> Keyword.merge(name: Dlex)
      |> Dlex.start_link()

    Weaver.Graph.reset!()
    :ok
  end

  describe "prepare" do
    @query """
    query {
      node(id: "TwitterUser:elixirdigest") {
        id
      }
    }
    """

    test "prepare" do
      assert %Weaver.Step{} = Weaver.prepare(@query)
    end
  end

  describe "3 levels without cursors" do
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
      user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
      favorites = build(ExTwitter.Model.Tweet, 10, fn i -> [id: 11 - i] end)

      {:ok, user: user, favorites: favorites}
    end

    test "works", %{user: user, favorites: favorites} do
      step = Weaver.prepare(@query)

      expect(Twitter, :user, fn "elixirdigest" -> user end)
      result = Weaver.weave(step)
      verify!()

      assert {data, [], [step2], nil} = result
      assert {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count} in data

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = step2

      assert user == step2.data

      # favorites initial
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
        assert user_id == user.id
        Enum.take(favorites, count)
      end)

      result = Weaver.weave(step2)
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
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 10},
               gap: nil
             } = step2_

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = step3a.ast
      assert step3a.data == tweet2b
      assert step3b.data == tweet2a

      # favorites pt. 2
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count, max_id: 9] ->
        assert user_id == user.id
        Enum.slice(favorites, 2, count)
      end)

      result = Weaver.weave(step2_)
      verify!()

      assert {data2_, meta2_, [step3c, step3d], step2__} = result

      [tweet2c, tweet2d] = Enum.slice(favorites, 2..3)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2c.id}"}} in data2_
      assert {%Ref{id: "Tweet:#{tweet2c.id}"}, "text", tweet2c.full_text} in data2_

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2d.id}"}} in data2_
      assert {%Ref{id: "Tweet:#{tweet2d.id}"}, "text", tweet2d.full_text} in data2_

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 8}
             } = step2__

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = step3c.ast
      assert step3c.data == tweet2d
      assert step3d.data == tweet2c
    end
  end

  describe "3 levels with cursors" do
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

    setup :use_graph

    setup do
      user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
      favorites = build(ExTwitter.Model.Tweet, 10, fn i -> [id: 11 - i] end)

      {:ok, user: user, favorites: favorites}
    end

    test "works", %{user: user, favorites: favorites} do
      meta = [
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 10, gap: false, ref: %Ref{id: "Tweet:10"}}},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 6, gap: true, ref: %Ref{id: "Tweet:6"}}}
      ]

      Weaver.Graph.store!([], meta)

      step = Weaver.prepare(@query, cache: Weaver.Graph)

      expect(Twitter, :user, fn "elixirdigest" -> user end)
      result = Weaver.weave(step)
      verify!()

      assert {data, [], [step2], nil} = result
      assert {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count} in data

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = step2

      assert user == step2.data

      # favorites initial
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
        assert user_id == user.id
        Enum.take(favorites, count)
      end)

      result = Weaver.weave(step2)
      verify!()

      assert {data2, meta2, dispatched, step2_} = result

      assert [step3a] = dispatched

      [tweet2a] = Enum.slice(favorites, 0..0)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2a.id}"}} in data2
      assert {%Ref{id: "Tweet:#{tweet2a.id}"}, "text", tweet2a.full_text} in data2

      # assert meta2 == [{%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Cursor{val: 10}}]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = step2_

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = step3a.ast
      assert step3a.data == tweet2a

      # favorites pt. 2
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count, max_id: 5] ->
        assert user_id == user.id
        start_index = 11 - 5
        Enum.slice(favorites, start_index, count)
      end)

      result = Weaver.weave(step2_)
      verify!()

      assert {data2_, meta2_, [step3c, step3d], step2__} = result

      [tweet2c, tweet2d] = Enum.slice(favorites, 11 - 5, 2)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2c.id}"}} in data2_
      assert {%Ref{id: "Tweet:#{tweet2c.id}"}, "text", tweet2c.full_text} in data2_

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2d.id}"}} in data2_
      assert {%Ref{id: "Tweet:#{tweet2d.id}"}, "text", tweet2d.full_text} in data2_

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 4}
             } = step2__

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = step3c.ast
      assert step3c.data == tweet2d
      assert step3d.data == tweet2c
    end
  end
end
