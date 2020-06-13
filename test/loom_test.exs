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

  setup do
    pid = self()

    callback = fn result = {data, meta, _, _, _}, dispatch_assigns, next_assigns ->
      Weaver.Graph.store!(data, meta)
      send(pid, {:callback, result, dispatch_assigns, next_assigns})
      {:ok, result, %{}, %{}}
    end

    {:ok, callback: callback}
  end

  test "weave", %{callback: callback} do
    user = build(TwitterUser, screen_name: "elixirdigest")
    expect(Twitter, :user, fn "elixirdigest" -> user end)

    Weaver.Loom.prepare(@query, Weaver.Absinthe.Schema, callback)
    |> Weaver.Loom.weave()

    assert_receive {:callback, _result, _dispatch_assigns, _next_assigns}, 10_000

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
    test "weave", %{callback: callback} do
      user = build(TwitterUser, screen_name: "elixirdigest")
      expect(Twitter, :user, fn "elixirdigest" -> user end)

      event = Weaver.Loom.prepare(@query, Weaver.Absinthe.Schema, callback)
      Weaver.Loom.Consumer.handle_events([event], self(), %{name: :weaver_consumer_x})

      assert_receive {:callback, _result, _dispatch_assigns, _next_assigns}, 10_000

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
