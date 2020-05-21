defmodule Weaver.Absinthe.AbsintheTest do
  use Weaver.IntegrationCase, async: false

  alias Weaver.ExTwitter.Mock, as: Twitter
  alias Weaver.Absinthe.Schema
  alias Weaver.Step.Result
  alias Weaver.Marker

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

  @invalid_query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        publishedAt
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

  setup do
    user = build(ExTwitter.Model.User, screen_name: "elixirdigest")
    favorites = build(ExTwitter.Model.Tweet, 4, fn i -> [id: 11 - i] end)

    {:ok, user: user, favorites: favorites}
  end

  test "works", %{user: user, favorites: favorites} do
    Mox.expect(Twitter, :user, fn "elixirdigest" -> user end)
    Mox.expect(Twitter, :favorites, twitter_mock_for(user, favorites))

    {:ok, result} =
      @query
      |> Weaver.Absinthe.run(Schema)

    assert Result.data(result) == [
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"}
           ]

    assert Result.meta(result) == []

    # |> IO.inspect()

    require IEx
    IEx.pry()

    [disp_favs, disp_tweets] = Result.dispatched(result)

    assert Result.next(result) == nil
    assert Result.meta(result) == []

    IO.puts("\n\nFAVORITES\n-==-0=-=-=-=-=-\n\n")
    {:ok, result_favs} = Weaver.Absinthe.resolve(disp_favs, Schema)

    [fav11, fav10 | _] = favorites

    assert Result.data(result_favs) == [
             {%Weaver.Ref{id: "Tweet:10"}, "text", fav10.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:10"}},
             {%Weaver.Ref{id: "Tweet:11"}, "text", fav11.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:11"}}
           ]

    assert Result.meta(result_favs) == [
             {:add, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              Marker.chunk_start("Tweet:11", 11)},
             {:add, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              Marker.chunk_end("Tweet:10", 10)}
           ]

    assert Result.dispatched(result_favs) == []
    assert next_favs = Result.next(result_favs)

    IO.puts("\n\nFAVORITES 2\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 9))
    {:ok, result_favs2} = Weaver.Absinthe.resolve(next_favs, Schema)

    [_, _, fav9, fav8 | _] = favorites

    assert Result.data(result_favs2) == [
             {%Weaver.Ref{id: "Tweet:8"}, "text", fav8.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:8"}},
             {%Weaver.Ref{id: "Tweet:9"}, "text", fav9.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:9"}}
           ]

    assert Result.meta(result_favs2) == [
             {:del, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              Marker.chunk_end("Tweet:10", 10)},
             {:add, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              Marker.chunk_end("Tweet:8", 8)}
           ]

    assert Result.dispatched(result_favs2) == []
    assert next_favs2 = Result.next(result_favs2)

    IO.puts("\n\nFAVORITES 3\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 7))
    {:ok, result_favs3} = Weaver.Absinthe.resolve(next_favs2, Schema)

    assert Result.data(result_favs3) == []

    assert Result.meta(result_favs3) == [
             {:del, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              Marker.chunk_end("Tweet:8", 8)}
           ]

    assert Result.dispatched(result_favs3) == []
    assert Result.next(result_favs3) == nil

    IO.puts("\n\nTWEETS\n-==-0=-=-=-=-=-\n\n")
    tweet1 = build(ExTwitter.Model.Tweet, id: 35)
    tweet2 = build(ExTwitter.Model.Tweet, id: 21)
    Mox.expect(Twitter, :user_timeline, fn _ -> [tweet1, tweet2] end)
    {:ok, result_tweets} = Weaver.Absinthe.resolve(disp_tweets, Schema)

    assert Result.data(result_tweets) == [
             {%Weaver.Ref{id: "Tweet:#{tweet2.id}"}, "text", tweet2.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              %Weaver.Ref{id: "Tweet:#{tweet2.id}"}},
             {%Weaver.Ref{id: "Tweet:#{tweet1.id}"}, "text", tweet1.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              %Weaver.Ref{id: "Tweet:#{tweet1.id}"}}
           ]

    assert Result.meta(result_tweets) == [
             {:add, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              Marker.chunk_start("Tweet:#{tweet1.id}", tweet1.id)},
             {:add, %Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              Marker.chunk_end("Tweet:#{tweet2.id}", tweet2.id)}
           ]

    assert [disp_retweets1, disp_retweets2] = Result.dispatched(result_tweets)
    assert Result.next(result_tweets)

    IO.puts("\n\nRETWEETS 1a\n-==-0=-=-=-=-=-\n\n")
    retweet1 = build(ExTwitter.Model.Tweet)
    tweet1_id = tweet1.id
    Mox.expect(Twitter, :retweets, fn ^tweet1_id, _ -> [retweet1] end)
    {:ok, result_retweets1} = Weaver.Absinthe.resolve(disp_retweets1, Schema)

    assert Result.data(result_retweets1) == [
             {%Weaver.Ref{id: "Tweet:#{retweet1.id}"}, "text", retweet1.full_text},
             {%Weaver.Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
              %Weaver.Ref{id: "Tweet:#{retweet1.id}"}}
           ]

    assert Result.meta(result_retweets1) == [
             {:add, %Weaver.Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
              Marker.chunk_start("Tweet:#{retweet1.id}", retweet1.id)},
             {:add, %Weaver.Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
              Marker.chunk_end("Tweet:#{retweet1.id}", retweet1.id)}
           ]

    assert Result.dispatched(result_retweets1) == []
    assert next_retweets1 = Result.next(result_retweets1)

    IO.puts("\n\nRETWEETS 2\n-==-0=-=-=-=-=-\n\n")
    retweet2 = build(ExTwitter.Model.Tweet)
    tweet2_id = tweet2.id
    Mox.expect(Twitter, :retweets, fn ^tweet2_id, _ -> [retweet2] end)
    {:ok, result_retweets2} = Weaver.Absinthe.resolve(disp_retweets2, Schema)

    assert Result.data(result_retweets2) == [
             {%Weaver.Ref{id: "Tweet:#{retweet2.id}"}, "text", retweet2.full_text},
             {%Weaver.Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
              %Weaver.Ref{id: "Tweet:#{retweet2.id}"}}
           ]

    assert Result.meta(result_retweets2) == [
             {:add, %Weaver.Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
              Marker.chunk_start("Tweet:#{retweet2.id}", retweet2.id)},
             {:add, %Weaver.Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
              Marker.chunk_end("Tweet:#{retweet2.id}", retweet2.id)}
           ]

    assert Result.dispatched(result_retweets2) == []
    assert next_retweets2 = Result.next(result_retweets2)

    IO.puts("\n\nRETWEETS 1b\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :retweets, fn ^tweet1_id, _ -> [] end)
    {:ok, result_retweets1b} = Weaver.Absinthe.resolve(next_retweets1, Schema)

    assert Result.data(result_retweets1b) == []

    assert Result.meta(result_retweets1b) == [
             {:del, %Weaver.Ref{id: "Tweet:#{tweet1.id}"}, "retweets",
              Marker.chunk_end("Tweet:#{retweet1.id}", retweet1.id)}
           ]

    assert Result.dispatched(result_retweets1b) == []
    assert Result.next(result_retweets1b) == nil

    IO.puts("\n\nRETWEETS 2b\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :retweets, fn ^tweet2_id, _ -> [] end)
    {:ok, result_retweets2b} = Weaver.Absinthe.resolve(next_retweets2, Schema)

    assert Result.data(result_retweets2b) == []

    assert Result.meta(result_retweets2b) == [
             {:del, %Weaver.Ref{id: "Tweet:#{tweet2.id}"}, "retweets",
              Marker.chunk_end("Tweet:#{retweet2.id}", retweet2.id)}
           ]

    assert Result.dispatched(result_retweets2b) == []
    assert Result.next(result_retweets2b) == nil
  end

  test "fails on invalid query", %{user: user, favorites: favorites} do
    {:ok, {:validation_failed, _}} =
      @invalid_query
      |> Weaver.Absinthe.run(Schema)
  end
end
