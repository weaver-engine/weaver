defmodule Weaver.NestedTimelinesTest do
  use Weaver.IntegrationCase, async: false

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        screenName
        favorites {
          text
        }
        tweets {
          text
          retweets {
            text
          }
        }
      }
    }
  }
  """

  setup do
    user = build(TwitterUser, screen_name: "elixirdigest")
    favorites = build(Tweet, 4, fn i -> [id: 11 - i] end)

    {:ok, user: user, favorites: favorites}
  end

  test "works", %{user: user, favorites: favorites} do
    user_ref = %Ref{id: "TwitterUser:#{user.screen_name}"}

    root_step =
      @query
      |> Weaver.prepare(Schema)
      |> weave_initial(Twitter, :user, fn "elixirdigest" -> user end)
      |> assert_data([
        {user_ref, "screenName", "elixirdigest"}
      ])
      |> assert_meta([])
      |> assert_dispatched_paths([
        [%{name: "favorites"}, %{name: "node"}, %{name: nil}],
        [%{name: "tweets"}, %{name: "node"}, %{name: nil}]
      ])
      |> refute_next()

    # FAVORITES
    [fav11, fav10, fav9, fav8 | _] = favorites

    root_step
    |> weave_dispatched(0, Twitter, :favorites, twitter_mock_for(user, favorites))
    |> assert_data([
      {%Ref{id: "Tweet:10"}, "text", fav10.full_text},
      {user_ref, "favorites", %Ref{id: "Tweet:10"}},
      {%Ref{id: "Tweet:11"}, "text", fav11.full_text},
      {user_ref, "favorites", %Ref{id: "Tweet:11"}}
    ])
    |> assert_meta([
      {:add, user_ref, "favorites", Marker.chunk_start("Tweet:11", 11)},
      {:add, user_ref, "favorites", Marker.chunk_end("Tweet:10", 10, 10)}
    ])
    |> assert_dispatched_paths([])
    |> assert_next_path([%{name: "favorites"}, %{name: "node"}, %{name: nil}])

    # FAVORITES 2
    |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 9))
    |> assert_data([
      {%Ref{id: "Tweet:8"}, "text", fav8.full_text},
      {user_ref, "favorites", %Ref{id: "Tweet:8"}},
      {%Ref{id: "Tweet:9"}, "text", fav9.full_text},
      {user_ref, "favorites", %Ref{id: "Tweet:9"}}
    ])
    |> assert_meta([
      {:del, user_ref, "favorites", Marker.chunk_end("Tweet:10", 10, 10)},
      {:add, user_ref, "favorites", Marker.chunk_end("Tweet:8", 8, 8)}
    ])
    |> assert_dispatched_paths([])
    |> assert_next_path([%{name: "favorites"}, %{name: "node"}, %{name: nil}])

    # FAVORITES 3
    |> weave_next(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 7))
    |> assert_data([])
    |> assert_meta([
      {:del, user_ref, "favorites", Marker.chunk_end("Tweet:8", 8, 8)}
    ])
    |> assert_dispatched_paths([])
    |> refute_next()

    # TWEETS
    tweet1 = build(Tweet, id: 35)
    tweet2 = build(Tweet, id: 21)

    retweets_step =
      root_step
      |> weave_dispatched(1, Twitter, :user_timeline, fn _ -> [tweet1, tweet2] end)
      |> assert_data([
        {%Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text},
        {user_ref, "tweets", %Ref{id: "Tweet:#{tweet2.id}"}},
        {%Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text},
        {user_ref, "tweets", %Ref{id: "Tweet:#{tweet1.id}"}}
      ])
      |> assert_meta([
        {:add, user_ref, "tweets", Marker.chunk_start("Tweet:#{tweet1.id}", tweet1.id)},
        {:add, user_ref, "tweets", Marker.chunk_end("Tweet:#{tweet2.id}", tweet2.id, tweet2.id)}
      ])
      |> assert_dispatched_paths([
        [%{name: "retweets"} | _],
        [%{name: "retweets"} | _]
      ])
      |> assert_next_path([%{name: "tweets"}, %{name: "node"}, %{name: nil}])
      |> assert_next_state(%{prev_chunk_end: %Marker{val: 21}})

    # RETWEETS 1a
    retweet1 = build(Tweet)
    tweet1_id = tweet1.id

    retweets_step
    |> weave_dispatched(0, Twitter, :retweets, fn ^tweet1_id, _ -> [retweet1] end)
    |> assert_data([
      {%Ref{id: "Tweet:#{retweet1.id}"}, "text", retweet1.full_text},
      {%Ref{id: "Tweet:#{tweet1.id}"}, "retweets", %Ref{id: "Tweet:#{retweet1.id}"}}
    ])
    |> assert_meta([
      {:add, %Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
       Marker.chunk_start("Tweet:#{retweet1.id}", retweet1.id)},
      {:add, %Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
       Marker.chunk_end("Tweet:#{retweet1.id}", retweet1.id, retweet1.id)}
    ])
    |> assert_dispatched_paths([])
    |> assert_next_path([%{name: "retweets"}, 0, %{name: "tweets"}, %{name: "node"}, %{name: nil}])

    # RETWEETS 1b
    |> weave_next(Twitter, :retweets, fn ^tweet1_id, _ -> [] end)
    |> assert_data([])
    |> assert_meta([
      {:del, %Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
       Marker.chunk_end("Tweet:#{retweet1.id}", retweet1.id, retweet1.id)}
    ])
    |> assert_dispatched_paths([])
    |> refute_next()

    # RETWEETS 2
    retweet2 = build(Tweet)
    tweet2_id = tweet2.id

    retweets_step
    |> weave_dispatched(1, Twitter, :retweets, fn ^tweet2_id, _ -> [retweet2] end)
    |> assert_data([
      {%Ref{id: "Tweet:#{retweet2.id}"}, "text", retweet2.full_text},
      {%Ref{id: "Tweet:#{tweet2.id}"}, "retweets", %Ref{id: "Tweet:#{retweet2.id}"}}
    ])
    |> assert_meta([
      {:add, %Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
       Marker.chunk_start("Tweet:#{retweet2.id}", retweet2.id)},
      {:add, %Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
       Marker.chunk_end("Tweet:#{retweet2.id}", retweet2.id, retweet2.id)}
    ])
    |> assert_dispatched_paths([])
    |> assert_next_path([%{name: "retweets"}, 1, %{name: "tweets"}, %{name: "node"}, %{name: nil}])

    # RETWEETS 2b
    |> weave_next(Twitter, :retweets, fn ^tweet2_id, _ -> [] end)
    |> assert_data([])
    |> assert_meta([
      {:del, %Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
       Marker.chunk_end("Tweet:#{retweet2.id}", retweet2.id, retweet2.id)}
    ])
    |> assert_dispatched_paths([])
    |> refute_next()
  end
end
