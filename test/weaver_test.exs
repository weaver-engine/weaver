defmodule WeaverTest do
  use ExUnit.Case, async: false

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      id
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
end
