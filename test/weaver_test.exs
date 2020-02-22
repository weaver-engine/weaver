defmodule WeaverTest do
  use Weaver.IntegrationCase, async: false

  alias Weaver.ExTwitter.Mock, as: Twitter

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
      [fav11, fav10, fav9, fav8 | _] = favorites

      @query
      |> Weaver.prepare()

      # user
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count}
      ])
      |> assert_meta([])
      |> assert_dispatched([
        %{
          ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
          cursor: nil,
          gap: :not_loaded,
          data: ^user
        }
      ])
      |> refute_next()

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, favorites))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav11.id}"}},
        {%Ref{id: "Tweet:#{fav11.id}"}, "text", fav11.full_text},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav10.id}"}},
        {%Ref{id: "Tweet:#{fav10.id}"}, "text", fav10.full_text}
      ])
      |> assert_meta([
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 11, gap: false, ref: %Ref{id: "Tweet:11"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 10, gap: true, ref: %Ref{id: "Tweet:10"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 10},
        gap: nil
      })
      |> assert_dispatched([
        %{data: ^fav10, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}},
        %{data: ^fav11, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 2
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 9))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav9.id}"}},
        {%Ref{id: "Tweet:#{fav9.id}"}, "text", fav9.full_text},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav8.id}"}},
        {%Ref{id: "Tweet:#{fav8.id}"}, "text", fav8.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 10, gap: true, ref: %Ref{id: "Tweet:10"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 8, gap: true, ref: %Ref{id: "Tweet:8"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 8}
      })
      |> assert_dispatched([
        %{data: ^fav8, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}},
        %{data: ^fav9, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])
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
      [fav21, _, _, _, _, _, fav15, fav14, fav13, _, fav11 | _] = favorites

      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favoritesCount", user.favourites_count}
      ])
      |> assert_meta([])
      |> assert_dispatched([
        %{
          ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
          cursor: nil,
          gap: :not_loaded,
          data: ^user
        }
      ])

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, favorites))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav21.id}"}},
        {%Ref{id: "Tweet:#{fav21.id}"}, "text", fav21.full_text}
      ])
      |> assert_meta([
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 21, gap: false, ref: %Ref{id: "Tweet:21"}}},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 20, gap: false, ref: %Ref{id: "Tweet:20"}}}
      ])
      |> assert_dispatched([
        %{data: ^fav21, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: nil,
        gap: :not_loaded
      })

      # favorites pt. 2
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 15))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav15.id}"}},
        {%Ref{id: "Tweet:#{fav15.id}"}, "text", fav15.full_text},
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav14.id}"}},
        {%Ref{id: "Tweet:#{fav14.id}"}, "text", fav14.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 16, gap: true, ref: %Ref{id: "Tweet:16"}}},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 14, gap: true, ref: %Ref{id: "Tweet:14"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 14}
      })
      |> assert_dispatched([
        %{data: ^fav14, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}},
        %{data: ^fav15, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 3
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 13))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav13.id}"}},
        {%Ref{id: "Tweet:#{fav13.id}"}, "text", fav13.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 14, gap: true, ref: %Ref{id: "Tweet:14"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 12}
      })
      |> assert_dispatched([
        %{data: ^fav13, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 4
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 11))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav11.id}"}},
        {%Ref{id: "Tweet:#{fav11.id}"}, "text", fav11.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 12, gap: true, ref: %Ref{id: "Tweet:12"}}},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 10, gap: false, ref: %Ref{id: "Tweet:10"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 12},
        gap: :not_loaded
      })
      |> assert_dispatched([
        %{data: ^fav11, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 5
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 7))
      |> assert_data([])
      |> assert_dispatched([])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 8, gap: true, ref: %Ref{id: "Tweet:8"}}},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         %Cursor{val: 7, gap: false, ref: %Ref{id: "Tweet:7"}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        cursor: %Cursor{val: 8},
        gap: :not_loaded
      })

      # favorites pt. 6
      |> weave_next()
      |> assert_done()
    end
  end
end
