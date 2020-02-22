defmodule Weaver.LoomTest do
  use Weaver.IntegrationCase, async: false

  import Mox

  alias Weaver.ExTwitter.Mock, as: Twitter
  alias ExTwitter.Model.User, as: TwitterUser

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      ... on TwitterUser {
        id
        screenName
      }
    }
  }
  """

  setup :set_mox_global
  setup :use_graph

  test "weave" do
    user = build(TwitterUser, screen_name: "elixirdigest")
    expect(Twitter, :user, fn "elixirdigest" -> user end)

    Weaver.Loom.weave(@query)

    :timer.sleep(500)

    query = ~s"""
    {
      user(func: eq(id, "TwitterUser:elixirdigest")) {
        screenName
      }
    }
    """

    assert {:ok, %{"user" => [%{"screenName" => "elixirdigest"}]}} = Dlex.query(Dlex, query)
  end

  describe "Consumer" do
    test "weave" do
      user = build(TwitterUser, screen_name: "elixirdigest")
      expect(Twitter, :user, fn "elixirdigest" -> user end)

      step = Weaver.prepare(@query)
      Weaver.Loom.Consumer.handle_events([step], self(), %{name: :weaver_consumer_x})

      :timer.sleep(500)

      query = ~s"""
      {
        user(func: eq(id, "TwitterUser:elixirdigest")) {
          screenName
        }
      }
      """

      assert {:ok, %{"user" => [%{"screenName" => "elixirdigest"}]}} = Dlex.query(Dlex, query)
    end
  end
end
