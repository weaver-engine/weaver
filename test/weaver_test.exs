defmodule WeaverTest do
  use Weaver.IntegrationCase, async: false

  import Test.Support.Factory
  import Mox

  alias Weaver.ExTwitter.Mock, as: Twitter

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

      assert {data, [], [dispatched], nil} = result
      assert {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count} in data

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = dispatched

      assert user == dispatched.data

      # favorites initial
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
        assert user_id == user.id
        Enum.take(favorites, count)
      end)

      result = Weaver.weave(dispatched)
      verify!()

      assert {data, meta, dispatched, next} = result

      assert [dispatched1, dispatched2] = dispatched

      [tweet1, tweet2] = Enum.slice(favorites, 0..1)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text} in data

      assert meta == [
               {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 11, gap: false, ref: %Ref{id: "Tweet:11"}}},
               {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 10, gap: true, ref: %Ref{id: "Tweet:10"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 10},
               gap: nil
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet2
      assert dispatched2.data == tweet1

      # favorites pt. 2
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count, max_id: 9] ->
        assert user_id == user.id
        Enum.slice(favorites, 2, count)
      end)

      result = Weaver.weave(next)
      verify!()

      assert {data, meta, [dispatched1, dispatched2], next} = result

      [tweet1, tweet2] = Enum.slice(favorites, 2..3)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text} in data

      assert meta == [
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 10, gap: true, ref: %Ref{id: "Tweet:10"}}},
               {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 8, gap: true, ref: %Ref{id: "Tweet:8"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 8}
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet2
      assert dispatched2.data == tweet1
    end
  end

  describe "3 levels with 1 gap" do
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
      favorites = build(ExTwitter.Model.Tweet, 20, fn i -> [id: 21 - i] end)

      meta = [
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 20, gap: false, ref: %Ref{id: "Tweet:20"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 16, gap: true, ref: %Ref{id: "Tweet:16"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 12, gap: true, ref: %Ref{id: "Tweet:12"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 10, gap: false, ref: %Ref{id: "Tweet:10"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 8, gap: true, ref: %Ref{id: "Tweet:8"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 7, gap: false, ref: %Ref{id: "Tweet:7"}}}
      ]

      Weaver.Graph.store!([], meta)

      {:ok, user: user, favorites: favorites}
    end

    test "works", %{user: user, favorites: favorites} do
      step = Weaver.prepare(@query, cache: Weaver.Graph)

      expect(Twitter, :user, fn "elixirdigest" -> user end)
      result = Weaver.weave(step)
      verify!()

      assert {data, [], [dispatched], nil} = result
      assert {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count} in data

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = dispatched

      assert user == dispatched.data

      assert Weaver.Graph.store!(data, [])

      # favorites initial
      expect(Twitter, :favorites, fn [id: user_id, tweet_mode: :extended, count: count] ->
        assert user_id == user.id
        Enum.take(favorites, count)
      end)

      result = Weaver.weave(dispatched)
      verify!()

      assert {data, meta, dispatched, next} = result

      assert [dispatched1] = dispatched

      [tweet1] = Enum.slice(favorites, 0..0)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert meta == [
               {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 21, gap: false, ref: %Ref{id: "Tweet:21"}}},
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 20, gap: false, ref: %Ref{id: "Tweet:20"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: nil,
               gap: :not_loaded
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet1

      assert Weaver.Graph.store!(data, meta)

      # favorites pt. 2
      expect(Twitter, :favorites, fn [
                                       id: user_id,
                                       tweet_mode: :extended,
                                       count: count,
                                       max_id: 15
                                     ] ->
        assert user_id == user.id
        start_index = 21 - 15
        Enum.slice(favorites, start_index, count)
      end)

      result = Weaver.weave(next)
      verify!()

      assert {data, meta, [dispatched1, dispatched2], next} = result

      [tweet1, tweet2] = Enum.slice(favorites, 21 - 15, 2)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet2.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text} in data

      assert meta == [
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 16, gap: true, ref: %Ref{id: "Tweet:16"}}},
               {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 14, gap: true, ref: %Ref{id: "Tweet:14"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 14}
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet2
      assert dispatched2.data == tweet1

      assert Weaver.Graph.store!(data, meta)

      # favorites pt. 3
      expect(Twitter, :favorites, fn [
                                       id: user_id,
                                       tweet_mode: :extended,
                                       count: count,
                                       max_id: 13
                                     ] ->
        assert user_id == user.id
        start_index = 21 - 13
        Enum.slice(favorites, start_index, count)
      end)

      result = Weaver.weave(next)
      verify!()

      assert {data, meta, [dispatched1], next} = result

      [tweet1] = Enum.slice(favorites, 21 - 13, 1)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert meta == [
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 14, gap: true, ref: %Ref{id: "Tweet:14"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 12}
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet1

      assert Weaver.Graph.store!(data, meta)

      # favorites pt. 4
      expect(Twitter, :favorites, fn [
                                       id: user_id,
                                       tweet_mode: :extended,
                                       count: count,
                                       max_id: 11
                                     ] ->
        assert user_id == user.id
        start_index = 21 - 11
        Enum.slice(favorites, start_index, count)
      end)

      result = Weaver.weave(next)
      verify!()

      assert {data, meta, [dispatched1], next} = result

      [tweet1] = Enum.slice(favorites, 21 - 11, 1)

      assert {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{tweet1.id}"}} in data
      assert {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text} in data

      assert meta == [
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 12, gap: true, ref: %Ref{id: "Tweet:12"}}},
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 10, gap: false, ref: %Ref{id: "Tweet:10"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 12},
               gap: :not_loaded
             } = next

      assert {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}} = dispatched1.ast
      assert dispatched1.data == tweet1

      assert Weaver.Graph.store!(data, meta)

      # favorites pt. 5
      expect(Twitter, :favorites, fn [
                                       id: user_id,
                                       tweet_mode: :extended,
                                       count: count,
                                       max_id: 7
                                     ] ->
        assert user_id == user.id
        start_index = 21 - 7
        Enum.slice(favorites, start_index, count)
      end)

      result = Weaver.weave(next)
      verify!()

      assert {[], meta, [], next} = result

      assert meta == [
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 8, gap: true, ref: %Ref{id: "Tweet:8"}}},
               {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
                %Cursor{val: 7, gap: false, ref: %Ref{id: "Tweet:7"}}}
             ]

      assert %{
               ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
               cursor: %Cursor{val: 8},
               gap: :not_loaded
             } = next

      assert Weaver.Graph.store!(data, meta)

      # favorites pt. 6
      result = Weaver.weave(next)
      verify!()

      assert {[], [], [], nil} = result
    end
  end
end
