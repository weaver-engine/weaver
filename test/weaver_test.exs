defmodule WeaverTest do
  use ExUnit.Case, async: false

  import Test.Support.Factory
  import Mox

  alias Weaver.Ref
  alias Weaver.ExTwitter.Mock, as: Twitter
  alias ExTwitter.Model.User, as: TwitterUser

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      id
    }
  }
  """

  setup do
    Mox.set_mox_global()
    {:ok, pid} = Weaver.Graph.start_link(nil)
    {:ok, pid} = Dlex.start_link(name: Dlex, port: 9081)
    Weaver.Graph.reset!()
    :ok
  end

  test "query" do
    query = Weaver.query(@query)

    refute query.result.errors
  end
end
