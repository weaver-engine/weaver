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
    tweet = build(ExTwitter.Model.Tweet)
    Mox.expect(Twitter, :user, fn "elixirdigest" -> user end)
    Mox.stub(Twitter, :favorites, fn _ -> favorites end)
    Mox.stub(Twitter, :user_timeline, fn _ -> [tweet] end)

    {:ok, result} =
      @query
      |> Weaver.Absinthe.run(Schema)

    assert Result.data(result) == [
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "screenName", "elixirdigest"}
           ]

    # |> IO.inspect()

    require IEx
    IEx.pry()

    [disp_favs, disp_retws] = Result.dispatched(result)

    IO.puts("\n\nFAVORITES\n-==-0=-=-=-=-=-\n\n")
    {:ok, result} = Weaver.Absinthe.resolve(disp_favs, Schema)

    [fav11, fav10 | _] = favorites

    assert Result.data(result) == [
             {%Weaver.Ref{id: "Tweet:10"}, "text", fav10.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:10"}},
             {%Weaver.Ref{id: "Tweet:11"}, "text", fav11.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "favorites",
              %Weaver.Ref{id: "Tweet:11"}}
           ]

    IO.puts("\n\nRETWEETS\n-==-0=-=-=-=-=-\n\n")
    {:ok, result} = Weaver.Absinthe.resolve(disp_retws, Schema)

    assert Result.data(result) == [
             {%Weaver.Ref{id: "Tweet:#{tweet.id}"}, "text", tweet.full_text},
             {%Weaver.Ref{id: "TwitterUser:elixirdigest"}, "tweets",
              %Weaver.Ref{id: "Tweet:#{tweet.id}"}}
           ]
  end
end
