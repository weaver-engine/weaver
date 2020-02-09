defmodule Weaver.LoomTest do
  use ExUnit.Case, async: false

  import Test.Support.Factory
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

  setup do
    Mox.set_mox_global()
    {:ok, _pid} = Weaver.Graph.start_link(nil)

    {:ok, _pid} =
      Application.get_env(:weaver, :dgraph, [])
      |> Keyword.merge(name: Dlex)
      |> Dlex.start_link()

    Weaver.Graph.reset!()
    :ok
  end

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
