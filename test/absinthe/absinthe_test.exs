defmodule Weaver.Absinthe.AbsintheTest do
  use Weaver.IntegrationCase, async: false

  alias Weaver.ExTwitter.Mock, as: Twitter
  alias Weaver.Absinthe.Schema
  alias Weaver.Step.Result

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
    tweet = build(ExTwitter.Model.Tweet)
    Mox.expect(Twitter, :user, fn "elixirdigest" -> user end)
    Mox.expect(Twitter, :favorites, twitter_mock_for(user, favorites))

    {:ok, result} =
      @query
      |> Weaver.Absinthe.run(Schema)

    assert Result.data(result) == [
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"}
           ]

    # |> IO.inspect()

    require IEx
    IEx.pry()

    [disp_favs, disp_tweets] = Result.dispatched(result)

    refute Result.next(result)

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

    assert Result.dispatched(result_favs2) == []
    assert next_favs2 = Result.next(result_favs2)

    IO.puts("\n\nFAVORITES 3\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :favorites, twitter_mock_for(user, favorites, max_id: 7))
    {:ok, result_favs3} = Weaver.Absinthe.resolve(next_favs2, Schema)

    assert Result.data(result_favs3) == []
    assert Result.dispatched(result_favs3) == []
    refute Result.next(result_favs3)

    IO.puts("\n\nTWEETS\n-==-0=-=-=-=-=-\n\n")
    Mox.expect(Twitter, :user_timeline, fn _ -> [tweet] end)
    {:ok, result_tweets} = Weaver.Absinthe.resolve(disp_tweets, Schema)

    assert Result.data(result_tweets) == [
             {%Weaver.Ref{id: "Tweet:#{tweet.id}"}, "text", tweet.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              %Weaver.Ref{id: "Tweet:#{tweet.id}"}}
           ]

    assert [disp_retweets] = Result.dispatched(result_tweets)
    assert Result.next(result_tweets)

    IO.puts("\n\nRETWEETS\n-==-0=-=-=-=-=-\n\n")
    retweet = build(ExTwitter.Model.Tweet)
    Mox.expect(Twitter, :retweets, fn _, _ -> [retweet] end)
    {:ok, result_retweets} = Weaver.Absinthe.resolve(disp_retweets, Schema)

    assert Result.data(result_retweets) == [
             {%Weaver.Ref{id: "Tweet:#{retweet.id}"}, "text", retweet.full_text},
             {%Weaver.Ref{id: "Tweet:#{tweet.id}"}, "retweets",
              %Weaver.Ref{id: "Tweet:#{retweet.id}"}}
           ]

    assert Result.dispatched(result_retweets) == []
    assert Result.next(result_retweets)
  end

  test "fails on invalid query", %{user: user, favorites: favorites} do
    {:ok, {:validation_failed, _}} =
      @invalid_query
      |> Weaver.Absinthe.run(Schema)
  end
end
