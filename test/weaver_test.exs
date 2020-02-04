defmodule WeaverTest do
  use ExUnit.Case, async: true

  @query """
  query {
    node(id: "TwitterUser:elixirdigest") {
      id
    }
  }
  """

  test "prepare" do
    assert %Weaver.Tree{} = Weaver.prepare(@query)
  end
end
