defmodule WeaverTest do
  @moduledoc """
  +-----------------------+-----------------------------------------------+----------------------------------------+
  |                       |            Initial marker (refresh)           |       Trailing marker (backfill)       |
  |                       +-------------------------+---------------------+--------------+-------------------------+
  |                       |       Marker range      | Singe-record marker | Marker range |   Single-record marker  |
  +-----------------------+-------------------------+---------------------+--------------+-------------------------+
  | Record added          | "with gaps" initial     |                     |              |                         |
  +-----------------------+-------------------------+---------------------+--------------+-------------------------+
  | No changes            |                         |                     |              |                         |
  +-----------------------+-------------------------+---------------------+--------------+-------------------------+
  | Marker record deleted | "deleted tweets" test 1 |                     |              | "deleted tweets" test 3 |
  +-----------------------+-------------------------+---------------------+--------------+-------------------------+
  | All records deleted   |                               "deleted tweets" last test                               |
  +-----------------------+----------------------------------------------------------------------------------------+
  """
  use Weaver.IntegrationCase, async: false

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
    test "prepare" do
      assert {:ok, %Weaver.Step{}} = Weaver.prepare(@query)
    end
  end

  describe "refresh: no marker, all records were deleted" do
  end

  describe "refresh: no marker, multiple new records retrieved" do
  end

  describe "refresh: no marker, one new record retrieved" do
  end

  describe "refresh: first chunk has one record, all records were deleted" do
  end

  describe "refresh: first chunk has one record, which is unchanged" do
  end

  describe "refresh: first chunk has one record, new record retrieved" do
  end

  describe "refresh: first chunk has one record, which was deleted" do
  end

  describe "refresh: first chunk has multiple records, all records were deleted" do
  end

  describe "refresh: first chunk has multiple records, records unchanged" do
  end

  describe "refresh: first chunk has multiple records, records added" do
  end

  describe "refresh: first chunk has multiple records, all of which were deleted" do
  end

  describe "refresh: first chunk has multiple records, some of which were deleted, multiple remaining" do
  end

  describe "refresh: first chunk has multiple records, some of which were deleted, one remaining" do
  end

  describe "backfill: first chunk has one record, no next chunk, all records were deleted" do
  end

  describe "backfill: first chunk has one record, no next chunk, multiple new records retrieved" do
  end

  describe "backfill: first chunk has one record, no next chunk, one new record retrieved" do
  end

  describe "backfill: first chunk has one record, next chunk has single record, all records were deleted" do
  end

  describe "backfill: first chunk has one record, next chunk has single record, gap closed" do
  end

  describe "backfill: first chunk has one record, next chunk has single record, multiple new records retrieved, gap closed" do
  end

  describe "backfill: first chunk has one record, next chunk has single record, multiple new records retrieved, gap not closed" do
  end

  describe "backfill: first chunk has one record, next chunk has single record, which was deleted" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, all records were deleted" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, gap closed" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, multiple new records retrieved, gap closed" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, multiple new records retrieved, gap not closed" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, all of which were deleted" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, some of which deleted, multiple remaining" do
  end

  describe "backfill: first chunk has one record, next chunk has multiple records, some of which deleted, one remaining" do
  end

  describe "backfill: first chunk has multiple records, no next chunk, all records were deleted" do
  end

  describe "backfill: first chunk has multiple records, no next chunk, multiple new records retrieved" do
  end

  describe "backfill: first chunk has multiple records, no next chunk, one new record retrieved" do
  end

  describe "backfill: first chunk has multiple records, next chunk has single record, all records were deleted" do
  end

  describe "backfill: first chunk has multiple records, next chunk has single record, gap closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has single record, multiple new records retrieved, gap closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has single record, multiple new records retrieved, gap not closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has single record, which was deleted" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, all records were deleted" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, gap closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, multiple new records retrieved, gap closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, multiple new records retrieved, gap not closed" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, all of which were deleted" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, some of which deleted, multiple remaining" do
  end

  describe "backfill: first chunk has multiple records, next chunk has multiple records, some of which deleted, one remaining" do
  end

  describe "without markers" do
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
          prev_chunk_end: nil,
          next_chunk_start: :not_loaded,
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
         Marker.chunk_start("Tweet:11", 11)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:10", 10)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: %Marker{type: :chunk_end, ref: %Ref{id: "Tweet:10"}, val: 10},
        next_chunk_start: nil
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
         Marker.chunk_end("Tweet:10", 10)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: %Marker{type: :chunk_end, ref: %Ref{id: "Tweet:8"}, val: 8}
      })
      |> assert_dispatched([
        %{data: ^fav8, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}},
        %{data: ^fav9, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])
    end
  end

  describe "with markers" do
    setup :use_graph

    setup do
      user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
      favorites = build(ExTwitter.Model.Tweet, 20, fn i -> [id: 21 - i] end)

      meta = [
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:16", 16)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
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
          prev_chunk_end: nil,
          next_chunk_start: :not_loaded,
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
         Marker.chunk_start("Tweet:21", 21)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)}
      ])
      |> assert_dispatched([
        %{data: ^fav21, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: :not_loaded,
        next_chunk_start: :not_loaded
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
         Marker.chunk_end("Tweet:16", 16)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:14", 14)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: %Marker{type: :chunk_end, ref: %Ref{id: "Tweet:14"}, val: 14}
      })
      |> assert_dispatched([
        %{data: ^fav14, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}},
        %{data: ^fav15, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 3 - gap closed
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 13))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav13.id}"}},
        {%Ref{id: "Tweet:#{fav13.id}"}, "text", fav13.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:14", 14)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: :not_loaded,
        next_chunk_start: :not_loaded
      })
      |> assert_dispatched([
        %{data: ^fav13, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 4 - gap closed
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 11))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav11.id}"}},
        {%Ref{id: "Tweet:#{fav11.id}"}, "text", fav11.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: :not_loaded,
        next_chunk_start: :not_loaded
      })
      |> assert_dispatched([
        %{data: ^fav11, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])

      # favorites pt. 5 - gap closed
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 7))
      |> assert_data([])
      |> assert_dispatched([])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: :not_loaded,
        next_chunk_start: :not_loaded
      })

      # favorites pt. 6
      |> weave_next()
      |> assert_done()
    end
  end

  describe "initial single-record chunk" do
    setup :use_graph

    setup do
      user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
      favorites = build(ExTwitter.Model.Tweet, 20, fn i -> [id: 21 - i] end)

      meta = [
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:20", 20)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:16", 16)}
      ]

      Weaver.Graph.store!([], meta)

      {:ok, user: user, favorites: favorites}
    end

    test "Refresh, record added", %{user: user, favorites: favorites} do
      [fav21 | _] = favorites

      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, favorites))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav21.id}"}},
        {%Ref{id: "Tweet:#{fav21.id}"}, "text", fav21.full_text}
      ])
      |> assert_meta([
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:21", 21)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)}
      ])
      |> assert_dispatched([
        %{data: ^fav21, ast: {:dispatched, {:field, {:name, _, "retweets"}, _, _, _, _, _}}}
      ])
      |> assert_next(%{
        ast: {:dispatched, {:field, {:name, _, "favorites"}, _, _, _, _, _}},
        prev_chunk_end: :not_loaded,
        next_chunk_start: :not_loaded
      })
    end
  end

  describe "deleted tweets" do
    setup :use_graph

    setup do
      user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
      favorites = build(ExTwitter.Model.Tweet, 20, fn i -> [id: 21 - i] end)

      meta = [
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:16", 16)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
      ]

      Weaver.Graph.store!([], meta)

      {:ok, user: user, favorites: favorites}
    end

    test "works with deleted tweet at start of next chunk", %{user: user, favorites: favorites} do
      [fav21, fav20 | _] = favorites

      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, fn [
                                                    id: user_id,
                                                    tweet_mode: :extended,
                                                    count: count
                                                  ] ->
        assert user_id == user.id
        [fav21, _ | tail] = favorites
        [fav21 | Enum.take(tail, count - 1)]
      end)
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav21.id}"}},
        {%Ref{id: "Tweet:#{fav21.id}"}, "text", fav21.full_text}
      ])
      |> refute_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav20.id}"}},
        {%Ref{id: "Tweet:#{fav20.id}"}, "text", fav20.full_text}
      ])
      |> assert_meta([
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:21", 21)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)}
      ])
    end

    test "deletes remaining markers with deleted remaining tweets (refresh)", %{
      user: user,
      favorites: favorites
    } do
      [fav21, fav20 | _] = favorites
      favorites = Enum.take(favorites, 1)

      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, favorites))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav21.id}"}},
        {%Ref{id: "Tweet:#{fav21.id}"}, "text", fav21.full_text}
      ])
      |> refute_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav20.id}"}},
        {%Ref{id: "Tweet:#{fav20.id}"}, "text", fav20.full_text}
      ])
      |> assert_meta([
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:21", 21)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:21", 21)}
      ])
      |> weave_next(
        Twitter,
        :favorites,
        twitter_mock_for(user, favorites, max_id: 20)
      )
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:21", 21)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:16", 16)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
      ])
      |> refute_next()
    end

    test "deletes remaining markers with deleted remaining tweets (backfill until next chunk)",
         %{
           user: user,
           favorites: favorites
         } do
      [_, _, _, _, _, _, fav15 | _] = favorites
      favorites = Enum.take(favorites, 7)

      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, favorites))

      # favorites pt. 2
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 15))
      |> assert_has_data([
        {%Ref{id: "TwitterUser:elixirdigest"}, "favorites", %Ref{id: "Tweet:#{fav15.id}"}},
        {%Ref{id: "Tweet:#{fav15.id}"}, "text", fav15.full_text}
      ])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:16", 16)},
        {:add, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:15", 15)}
      ])

      # favorites last pt.
      |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 14))
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:15", 15)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
      ])
      |> refute_next()
    end

    test "deletes all markers with all tweets deleted", %{user: user} do
      @query
      |> Weaver.prepare(cache: Weaver.Graph)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)

      # favorites initial
      |> weave_dispatched(Twitter, :favorites, twitter_mock_for(user, []))
      |> assert_data([])
      |> assert_meta([
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:20", 20)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:16", 16)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_end("Tweet:12", 12)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:10", 10)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites", Marker.chunk_end("Tweet:8", 8)},
        {:del, %Ref{id: "TwitterUser:elixirdigest"}, "favorites",
         Marker.chunk_start("Tweet:7", 7)}
      ])
      |> refute_next()
    end
  end
end
