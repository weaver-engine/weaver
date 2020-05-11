defmodule Weaver.Absinthe.AbsintheTest do
  use Weaver.IntegrationCase, async: false

  alias Weaver.ExTwitter.Mock, as: Twitter
  alias Weaver.Absinthe.Schema

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        favorites {
          text
        }
        retweets {
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
    Mox.expect(Twitter, :user, fn "elixirdigest" -> user end)
    Mox.expect(Twitter, :favorites, fn _ -> favorites end)
    Mox.stub(Twitter, :retweets, fn _, _ -> [build(ExTwitter.Model.Tweet)] end)

    @query
    |> Weaver.Absinthe.run(Schema)
  end
end
