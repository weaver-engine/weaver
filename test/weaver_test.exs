defmodule WeaverTest do
  use ExUnit.Case, async: false

  import Test.Support.Factory
  import Mox

  alias Weaver.ExTwitter.Mock, as: Twitter
  alias ExTwitter.Model.User, as: TwitterUser

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      id
    }
  }
  """

  @query2 """
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

  test "parse" do
    assert {_ast, _fun_env} = Weaver.parse_query(@query)
  end

  test "weave" do
    user = build(TwitterUser, screen_name: "elixirdigest")
    expect(Twitter, :user, fn "elixirdigest" -> user end)

    Weaver.weave(@query2)

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
